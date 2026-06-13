"""
qualify_master — Worker LiveKit OUTBOUND a FREDDO (canale Digital Revenue), multi-tenant.

Worker self-hosted (Railway) connesso a LiveKit Cloud (progetto odyra-poc).
Chiama lead freddi, riconosce SUBITO se ha risposto una persona o una segreteria
(AMD nativo LiveKit) e, se è una persona, esegue la qualifica a freddo del master
`qualify_cold`. Se è segreteria, riaggancia senza lasciare messaggi.

Differenze chiave rispetto a Eva (scheletro di partenza):
  1. La chiamata SIP si crea DENTRO il worker, dopo aver aperto il context manager
     AMD (NON nel dispatcher), così l'AMD non perde la frase iniziale della segreteria.
  2. UN SOLO @function_tool: knowledge_query. Qualifica/opt-out viaggiano TUTTI
     nell'End-of-Call report verso /webhook/dr-end-of-call (senza auth). n8n estrae
     hotness/summary/answers/esito dal transcript.
  3. Il system prompt arriva nei metadata (skeleton_prompt + variables): il worker fa
     solo fill_prompt({{...}}), come Eva. Niente prompt hardcoded, niente Supabase.

agent_name = "qualify_master" (combacia con tenants.livekit_agent_name nel DB).

Tutto è parametrizzabile via env (voce, modello voce, lingua, speed, volume, timeout,
soglie AMD, modelli LLM/STT): vedi sezione Config e .env.example.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re

import aiohttp
from dotenv import load_dotenv

from livekit import api
from livekit.agents import (
    AMD,
    Agent,
    AgentSession,
    AudioConfig,
    BackgroundAudioPlayer,
    BuiltinAudioClip,
    JobContext,
    JobProcess,
    RunContext,
    WorkerOptions,
    cli,
    function_tool,
)
from livekit.plugins import cartesia, deepgram, openai, silero
from livekit.plugins.turn_detector.multilingual import MultilingualModel

load_dotenv()

logger = logging.getLogger("qualify_master")
logging.basicConfig(level=logging.INFO)

# ───────────────────────── Identità agente ─────────────────────────

AGENT_NAME = os.getenv("LIVEKIT_AGENT_NAME", "qualify_master")

# ───────────────────────── LLM ─────────────────────────

LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4.1-mini")
LLM_FALLBACK_MODEL = os.getenv("LLM_FALLBACK_MODEL", "claude-sonnet-4-6")
LLM_TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "0.3"))

# ───────────────────────── TTS (Cartesia) ─────────────────────────

CARTESIA_MODEL = os.getenv("CARTESIA_MODEL", "sonic-3") or "sonic-3"
CARTESIA_VOICE = os.getenv("CARTESIA_VOICE", "36d94908-c5b9-4014-b521-e69aee5bead0")
CARTESIA_LANGUAGE = os.getenv("CARTESIA_LANGUAGE", "it")
CARTESIA_SPEED = float(os.getenv("CARTESIA_SPEED", "1.1"))
CARTESIA_VOLUME = float(os.getenv("CARTESIA_VOLUME", "1.2"))

# ───────────────────────── STT (Deepgram) ─────────────────────────

DEEPGRAM_MODEL = os.getenv("DEEPGRAM_MODEL", "nova-3-general")
DEEPGRAM_LANGUAGE = os.getenv("DEEPGRAM_LANGUAGE", "it")
DEEPGRAM_NUMERALS = os.getenv("DEEPGRAM_NUMERALS", "true").lower() in ("1", "true", "yes")

# Deepgram nova-3 italiano: keyterm list utile per email/numeri (come Eva).
KEYTERMS = [
    "chiocciola", "punto", "virgola", "trattino", "trattino basso", "underscore",
    "gmail", "yahoo", "hotmail", "outlook", "libero", "alice", "tim", "virgilio",
    "icloud", "protonmail", "email", "posta elettronica", "indirizzo mail",
    "prefisso", "cellulare", "numero di telefono",
    "zero", "uno", "due", "tre", "quattro", "cinque", "sei", "sette", "otto", "nove",
]

# ───────────────────────── SIP outbound (trunk NUOVO dedicato al freddo) ─────────────────────────

SIP_OUTBOUND_TRUNK_ID = os.getenv("SIP_OUTBOUND_TRUNK_ID", "")
SIP_FROM_NUMBER = os.getenv("SIP_FROM_NUMBER", "")

# ───────────────────────── AMD (rilevamento segreteria) ─────────────────────────

def _envf(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _envb(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).lower() in ("1", "true", "yes")


AMD_ENABLED = _envb("AMD_ENABLED", True)
AMD_IVR_HANGUP = _envb("AMD_IVR_HANGUP", True)
AMD_LLM_MODEL = os.getenv("AMD_LLM_MODEL", "gpt-4.1-mini")
AMD_HUMAN_SPEECH_THRESHOLD_S = _envf("AMD_HUMAN_SPEECH_THRESHOLD_S", 2.5)
AMD_HUMAN_SILENCE_THRESHOLD_S = _envf("AMD_HUMAN_SILENCE_THRESHOLD_S", 0.5)
AMD_MACHINE_SILENCE_THRESHOLD_S = _envf("AMD_MACHINE_SILENCE_THRESHOLD_S", 1.5)
AMD_NO_SPEECH_THRESHOLD_S = _envf("AMD_NO_SPEECH_THRESHOLD_S", 10.0)
AMD_TIMEOUT_S = _envf("AMD_TIMEOUT_S", 20.0)

# Prompt di classificazione in ITALIANO (override via env AMD_IT_PROMPT).
# Bias verso "nel dubbio è una persona": una macchina si riconosce solo da formule
# lunghe e impersonali tipiche delle segreterie italiane; tutto il resto è umano.
AMD_IT_PROMPT_DEFAULT = (
    "Stai classificando la PRIMA risposta a una telefonata in uscita, in italiano. "
    "Decidi se a rispondere è una PERSONA o una SEGRETERIA/risponditore automatico.\n\n"
    "È una SEGRETERIA (machine) se senti formule lunghe e impersonali come: "
    "\"il numero selezionato non è al momento raggiungibile\", "
    "\"la persona da lei chiamata non è al momento disponibile\", "
    "\"si prega di lasciare un messaggio dopo il segnale acustico\", "
    "\"la segreteria telefonica di...\", \"avete chiamato il numero...\", "
    "messaggi degli operatori (TIM, Vodafone, WindTre, Iliad, Ho, Fastweb), "
    "musica/jingle dell'operatore, un beep/segnale acustico, oppure un menu IVR "
    "(\"digiti 1\", \"prema il tasto...\").\n\n"
    "È una PERSONA (human) se senti un saluto breve e diretto come "
    "\"Pronto?\", \"Sì?\", \"Chi parla?\", \"Chi è?\", \"Buongiorno\", il proprio nome, "
    "oppure qualsiasi risposta colloquiale e spontanea.\n\n"
    "REGOLA DI SICUREZZA: scambiare una persona per una macchina è l'errore peggiore. "
    "Davanti a un \"Pronto?\" secco, un silenzio breve o qualcosa di ambiguo, classifica "
    "SEMPRE come PERSONA. Considera SEGRETERIA solo quando le formule impersonali sono chiare."
)
AMD_IT_PROMPT = os.getenv("AMD_IT_PROMPT", AMD_IT_PROMPT_DEFAULT)

# ───────────────────────── Webhook / RAG ─────────────────────────

CORE_BASE = os.getenv("CORE_BASE_URL", "https://primary-production-eb20.up.railway.app/webhook")
RAG_URL = os.getenv("RAG_URL", "https://rag-production-6ab5.up.railway.app/")
EOC_WEBHOOK_URL = os.getenv(
    "EOC_WEBHOOK_URL",
    "https://primary-production-eb20.up.railway.app/webhook/dr-end-of-call",
)

# ───────────────────────── Timeout / turni ─────────────────────────

SILENCE_TIMEOUT_S = _envf("SILENCE_TIMEOUT_S", 30.0)
MAX_DURATION_S = _envf("MAX_DURATION_S", 600.0)
STUCK_TIMEOUT_S = _envf("STUCK_TIMEOUT_S", 15.0)
EMPTY_TURN_REPROMPT_S = _envf("EMPTY_TURN_REPROMPT_S", 8.0)
# Anti-mutezza (Caso B): con un lead freddo non si insiste → un solo reprompt.
# EMPTY_TURN_ENABLED=false spegne del tutto la pianificazione del check.
# Ordine dei tempi richiesto: EMPTY_TURN_REPROMPT_S < STUCK_TIMEOUT_S < SILENCE_TIMEOUT_S.
EMPTY_TURN_ENABLED = _envb("EMPTY_TURN_ENABLED", True)
EMPTY_TURN_MAX_REPROMPTS = 1
EMPTY_TURN_PHRASE = os.getenv("EMPTY_TURN_PHRASE", "Mi sente?")
MIN_ENDPOINTING_DELAY = _envf("MIN_ENDPOINTING_DELAY", 0.6)
MAX_ENDPOINTING_DELAY = _envf("MAX_ENDPOINTING_DELAY", 2.0)

# Tempo massimo di attesa che il participant SIP si connetta (oltre l'AMD timeout).
ANSWER_WAIT_S = _envf("ANSWER_WAIT_S", 90.0)

# ───────────────────────── Costo ─────────────────────────

COST_PER_MINUTE_EUR = float(os.getenv("EOC_COST_PER_MINUTE_EUR", "0") or 0)

# ───────────────────────── Filler vocali + musichetta (pattern Eva) ─────────────────────────

FILLER_KNOWLEDGE = "Mi lasci un istante, le verifico subito questa informazione."
FILLER_DELAY_S = float(os.getenv("FILLER_DELAY_S", "0.7"))

THINKING_SOUND = AudioConfig(
    BuiltinAudioClip.HOLD_MUSIC, volume=0.4, fade_in=0.3, fade_out=0.5
)

PLACEHOLDER_RE = re.compile(r"\{\{\s*([^}]+?)\s*\}\}")
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=20)


# ───────────────────────── Helpers ─────────────────────────

def fill_prompt(template: str, variables: dict) -> str:
    """Sostituisce ogni {{chiave}} con variables["chiave"]; mancante → stringa vuota.
    Identico al meccanismo Eva (EVA_system_prompt + variableValues)."""
    return PLACEHOLDER_RE.sub(lambda m: str(variables.get(m.group(1).strip(), "")), template)


def _slug(value: str) -> str:
    return "".join(c if c.isalnum() else "-" for c in str(value)).strip("-") or "lead"


async def _post_json(url: str, payload: dict) -> str:
    """POST JSON, ritorna il testo della risposta (o errore leggibile dal modello)."""
    try:
        async with aiohttp.ClientSession(timeout=HTTP_TIMEOUT) as s:
            async with s.post(url, json=payload) as resp:
                text = await resp.text()
                logger.info("POST %s -> %s", url, resp.status)
                return text
    except Exception as e:  # noqa: BLE001
        logger.exception("POST %s failed", url)
        return json.dumps({"error": str(e)})


# call_outcome di default derivato dall'ended_reason. La qualifica fine
# (qualified/not_interested/...) e l'opt-out li estrae n8n dal transcript.
_OUTCOME_BY_REASON = {
    "completed": "completed",
    "customer_hangup": "completed",
    "silence_timeout": "completed",
    "max_duration": "completed",
    "stuck": "completed",
    "voicemail": "voicemail",
    "no_answer": "no_answer",
    "failed": "failed",
}


# ───────────────────────── Agent (1 SOLO tool) ─────────────────────────

class QualifyAgent(Agent):
    """Agente qualify_cold a freddo. `tenant_id` e `phone` NON sono argomenti del
    modello: vengono iniettati dai metadata della room."""

    def __init__(self, instructions: str, md: dict) -> None:
        super().__init__(instructions=instructions)
        self.md = md
        self._filler_handle = None
        self._filler_armed = True

    # --- accessor metadata (mai esposti al modello) ---
    @property
    def tenant_id(self) -> str:
        return str(self.md.get("tenant_id", ""))

    @property
    def lead_phone(self) -> str:
        return str(self.md.get("phone", ""))

    @property
    def lead_name(self) -> str:
        return str(self.md.get("lead_name", ""))

    async def _fill_then(self, context: RunContext, phrase: str, coro):
        """Esegue il lavoro del tool e, SOLO se supera FILLER_DELAY_S, dice la frase
        d'attesa (max 1 per turno). Sotto soglia la pausa la copre la musichetta."""
        filler_task = asyncio.create_task(self._delayed_filler(context, phrase))
        try:
            return await coro
        finally:
            filler_task.cancel()

    async def _delayed_filler(self, context: RunContext, phrase: str) -> None:
        try:
            await asyncio.sleep(FILLER_DELAY_S)
        except asyncio.CancelledError:
            return
        try:
            if not self._filler_armed:
                return
            if self._filler_handle is not None and not self._filler_handle.done():
                return
            self._filler_armed = False
            self._filler_handle = context.session.say(
                phrase, allow_interruptions=True, add_to_chat_ctx=False
            )
        except Exception as e:  # noqa: BLE001
            logger.debug("filler skipped: %s", e)

    # ───────── UNICO tool: knowledge_query ─────────

    @function_tool()
    async def knowledge_query(self, context: RunContext, query: str) -> str:
        """Rispondi a domande del cliente su prodotto, servizio, prezzi, condizioni,
        zone, promozioni, FAQ dell'azienda. `query` = la frase COMPLETA del cliente
        (mai una sola parola)."""
        # Host RAG diverso dal base n8n; campo `id` = tenant_id (NON `tenant_id`).
        return await self._fill_then(
            context, FILLER_KNOWLEDGE,
            _post_json(RAG_URL, {"id": self.tenant_id, "query": query}))


# ───────────────────────── TTS builder ─────────────────────────

def build_tts():
    return cartesia.TTS(
        model=CARTESIA_MODEL,
        voice=CARTESIA_VOICE,
        language=CARTESIA_LANGUAGE,
        speed=CARTESIA_SPEED,
        volume=CARTESIA_VOLUME,
        word_timestamps=False,
    )


# ───────────────────────── LLM builder (con fallback Anthropic) ─────────────────────────

def build_llm():
    primary = openai.LLM(model=LLM_MODEL, temperature=LLM_TEMPERATURE, parallel_tool_calls=False)
    # Fallback ad Anthropic Sonnet se la chiave è presente e il plugin è installato.
    if os.getenv("ANTHROPIC_API_KEY"):
        try:
            from livekit.agents.llm import FallbackAdapter
            from livekit.plugins import anthropic
            fallback = anthropic.LLM(model=LLM_FALLBACK_MODEL, temperature=LLM_TEMPERATURE)
            return FallbackAdapter([primary, fallback])
        except Exception as e:  # noqa: BLE001
            logger.warning("Fallback Anthropic non disponibile (%s); uso solo OpenAI", e)
    return primary


# ───────────────────────── Worker lifecycle ─────────────────────────

def prewarm(proc: JobProcess) -> None:
    proc.userdata["vad"] = silero.VAD.load()


async def _hangup(ctx: JobContext) -> None:
    """Chiude la chiamata cancellando la room (fa scattare gli shutdown callback → EOC)."""
    try:
        await ctx.api.room.delete_room(api.DeleteRoomRequest(room=ctx.room.name))
    except Exception:  # noqa: BLE001
        logger.exception("delete_room fallita")


async def _cancel(task: asyncio.Task) -> None:
    task.cancel()


def _build_transcript(session: AgentSession) -> list[dict]:
    """Serializza la cronologia come [{role, text}] per il payload EOC del DR."""
    out: list[dict] = []
    try:
        for item in session.history.items:
            role = getattr(item, "role", None)
            content = getattr(item, "content", None)
            if not role or content is None:
                continue
            if isinstance(content, list):
                text = " ".join(c for c in content if isinstance(c, str)).strip()
            else:
                text = str(content).strip()
            if role not in ("user", "assistant"):
                continue
            if text:
                out.append({"role": role, "text": text})
    except Exception:  # noqa: BLE001
        logger.exception("Impossibile costruire la trascrizione")
    return out


async def _create_sip(ctx: JobContext, phone: str, participant_identity: str, lead_name: str) -> None:
    """Crea il participant SIP in uscita. wait_until_answered=True è OBBLIGATORIO per l'AMD."""
    await ctx.api.sip.create_sip_participant(
        api.CreateSIPParticipantRequest(
            room_name=ctx.room.name,
            sip_trunk_id=SIP_OUTBOUND_TRUNK_ID,
            sip_call_to=phone,
            participant_identity=participant_identity,
            participant_name=lead_name or "lead",
            wait_until_answered=True,
        )
    )


async def entrypoint(ctx: JobContext) -> None:
    # 1) Metadata della room (costruiti da n8n)
    try:
        md = json.loads(ctx.job.metadata or "{}")
    except json.JSONDecodeError:
        logger.error("Metadata non è JSON valido; uso dict vuoto")
        md = {}
    logger.info("qualify_master job tenant=%s lead=%s", md.get("tenant_id"), md.get("lead_id"))

    # 2) System prompt: skeleton_prompt + variables dai metadata → fill_prompt({{...}})
    #    Le variables hanno precedenza; i campi top-level (lead_name, current_context, ...)
    #    restano disponibili come fallback per i placeholder.
    skeleton = str(md.get("skeleton_prompt", ""))
    fill_vars = {**md, **(md.get("variables") or {})}
    system_prompt = fill_prompt(skeleton, fill_vars)
    if not skeleton:
        logger.warning("skeleton_prompt assente nei metadata: prompt vuoto")

    await ctx.connect()

    # 3) Sessione (STT/LLM/TTS/VAD/turn) — tutto da env
    vad = ctx.proc.userdata.get("vad") or silero.VAD.load()
    session = AgentSession(
        stt=deepgram.STT(
            model=DEEPGRAM_MODEL,
            language=DEEPGRAM_LANGUAGE,
            numerals=DEEPGRAM_NUMERALS,
            keyterm=KEYTERMS,
        ),
        llm=build_llm(),
        tts=build_tts(),
        vad=vad,
        turn_detection=MultilingualModel(),
        min_endpointing_delay=MIN_ENDPOINTING_DELAY,
        max_endpointing_delay=MAX_ENDPOINTING_DELAY,
    )

    agent = QualifyAgent(instructions=system_prompt, md=md)

    # ── stato chiamata (per EOC) ──
    loop = asyncio.get_running_loop()
    state = {
        "ended_reason": "completed",
        "amd_category": None,
        "amd_transcript": "",
        "start": loop.time(),
        "last_activity": loop.time(),
        "closing": False,            # guard: una sola chiusura
        "conversation_started": False,  # rete anti-mutezza attiva solo a conversazione avviata
    }

    # Stato "mai muto" per il turno a vuoto (Caso B, portato da Boss). got_final: visto un
    # final nel turno corrente; reprompts: re-prompt consecutivi; task: check pendente.
    empty_turn_state = {"got_final": False, "reprompts": 0, "task": None}

    def _touch(*_args) -> None:
        state["last_activity"] = loop.time()

    def _trigger_close(reason: str) -> None:
        # Chiusura centralizzata e idempotente: imposta ended_reason e riaggancia una volta.
        if state["closing"]:
            return
        state["closing"] = True
        state["ended_reason"] = reason
        logger.info("[CLOSE] reason=%s → hangup", reason)
        asyncio.create_task(_hangup(ctx))

    async def _empty_turn_check() -> None:
        # Caso B (turno a vuoto): pianificato quando l'utente smette di parlare. Dopo l'attesa,
        # se NON è arrivato alcun final E l'agente non sta elaborando/parlando E l'utente non
        # ha ripreso a parlare → nessuna trascrizione utile: un reprompt invece di restare muti.
        try:
            await asyncio.sleep(EMPTY_TURN_REPROMPT_S)
        except asyncio.CancelledError:
            return
        if state["closing"] or not state["conversation_started"]:
            return
        if empty_turn_state["got_final"]:
            return  # un final è arrivato → niente reprompt
        if session.agent_state in ("thinking", "speaking"):
            return  # l'agente sta già rispondendo
        if session.user_state != "listening":
            return  # l'utente ha ripreso a parlare / è 'away'
        if empty_turn_state["reprompts"] >= EMPTY_TURN_MAX_REPROMPTS:
            return  # niente loop: con un lead freddo basta un reprompt, poi lascia chiudere
        empty_turn_state["reprompts"] += 1
        logger.info(
            "[EMPTY-TURN] nessuna trascrizione utile → reprompt (%d/%d)",
            empty_turn_state["reprompts"], EMPTY_TURN_MAX_REPROMPTS,
        )
        try:
            await session.say(EMPTY_TURN_PHRASE, allow_interruptions=True)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            logger.error("[EMPTY-TURN] reprompt failed: %s", e)

    async def _cancel_empty_turn() -> None:
        # Cleanup alla chiusura: annulla il check del turno a vuoto se pendente.
        t = empty_turn_state["task"]
        if t is not None and not t.done():
            t.cancel()

    # user_input_transcribed: aggiorna stato anti-mutezza + ri-arma il filler (max 1/turno).
    def _on_user_text(ev) -> None:
        if getattr(ev, "is_final", False):
            empty_turn_state["got_final"] = True   # turno con trascrizione → no reprompt
            empty_turn_state["reprompts"] = 0       # turno riuscito → azzera i reprompt
            if (getattr(ev, "transcript", "") or "").strip():
                state["last_activity"] = loop.time()  # reset silence SOLO su parlato reale
            agent._filler_armed = True

    session.on("user_input_transcribed", _on_user_text)

    # agent_state_changed: attività → reset del timer di silenzio.
    def _on_agent_state(ev) -> None:
        _touch()

    session.on("agent_state_changed", _on_agent_state)

    # user_state_changed: reset silence + rete "mai muto" (pianifica/annulla l'empty-turn check).
    def _on_user_state(ev) -> None:
        _touch()
        if not (EMPTY_TURN_ENABLED and state["conversation_started"]):
            return
        new_state = getattr(ev, "new_state", None)
        if new_state == "speaking":
            # Nuovo turno utente: azzera got_final e annulla l'eventuale check pendente.
            empty_turn_state["got_final"] = False
            t = empty_turn_state["task"]
            if t is not None and not t.done():
                t.cancel()
            empty_turn_state["task"] = None
        elif new_state == "listening":
            # L'utente ha smesso di parlare: pianifica il check dopo EMPTY_TURN_REPROMPT_S.
            t = empty_turn_state["task"]
            if t is not None and not t.done():
                t.cancel()
            empty_turn_state["task"] = asyncio.create_task(
                _empty_turn_check(), name="empty_turn_check"
            )

    session.on("user_state_changed", _on_user_state)

    # Il lead riaggancia → chiusura con motivo dedicato.
    def _on_disconnect(participant) -> None:
        ident = getattr(participant, "identity", "")
        if ident.startswith("sip-") or str(getattr(participant, "kind", "")).upper().endswith("SIP"):
            _trigger_close("customer_hangup")

    ctx.room.on("participant_disconnected", _on_disconnect)

    # 4) End-of-call: POST al webhook EOC del DR alla chiusura del job (payload sezione 6).
    async def _send_eoc() -> None:
        duration = max(0, int(loop.time() - state["start"]))
        ended_reason = state["ended_reason"]
        payload = {
            "tenant_id": md.get("tenant_id"),
            "lead_id": md.get("lead_id"),
            "campaign_id": md.get("campaign_id"),
            "call_id": ctx.room.name,
            "vonage_call_uuid": None,
            "call_outcome": _OUTCOME_BY_REASON.get(ended_reason, "completed"),
            # hotness/summary/answers li estrae n8n dal transcript a fine call.
            "hotness": None,
            "answers": {},
            "summary": "",
            "next_step": "",
            "duration_seconds": duration,
            "cost_eur": round(duration / 60.0 * COST_PER_MINUTE_EUR, 4),
            "cost_vonage_eur": 0.0,
            "caller_number": md.get("caller_number") or SIP_FROM_NUMBER,
            "retry_count": int(md.get("retry_count", md.get("attempt_number", 0)) or 0),
            "transcript": _build_transcript(session),
            "ended_reason": ended_reason,
            "amd_category": state["amd_category"],
            "amd_transcript": state["amd_transcript"],
            "agent": AGENT_NAME,
        }
        logger.info(
            "EOC POST reason=%s outcome=%s amd=%s dur=%ss",
            ended_reason, payload["call_outcome"], state["amd_category"], duration,
        )
        await _post_json(EOC_WEBHOOK_URL, payload)

    ctx.add_shutdown_callback(_send_eoc)

    # 5) Avvia la sessione. La musichetta "thinking" NON parte qui: verrebbe analizzata
    #    dall'AMD (inquinando l'audio) e udita da una persona in apertura o su una segreteria.
    #    Si avvia solo dopo il blocco AMD, quando c'è davvero una conversazione con un umano.
    await session.start(agent=agent, room=ctx.room)

    # 6) Identità SIP + numero (ora dial-a il worker, non il dispatcher)
    phone = str(md.get("phone", "")).strip()
    lead_id = md.get("lead_id") or md.get("tenant_id") or "lead"
    participant_identity = f"sip-{_slug(lead_id)}"
    if not phone:
        logger.error("phone assente nei metadata: impossibile chiamare")
        state["ended_reason"] = "failed"
        await _hangup(ctx)
        return

    # 7) Costruisce la frase di apertura PRIMA del blocco AMD (serve per il barge-in).
    lead_name_barge = agent.lead_name or ""
    nome_parte_barge = f", {lead_name_barge}" if lead_name_barge else ""
    # Frase brevissima: meno parole = meno latenza TTS = risposta quasi istantanea.
    # A FREDDO non usiamo il nome del lead: non lo conosciamo davvero.
    _agent_name = md.get('agent_name', 'Laura')
    _azienda = md.get('azienda', 'Fresco Casa')
    frase_apertura = f"Buongiorno, sono {_agent_name} di {_azienda}!"

    # 8) BLOCCO AMD — la chiamata SIP si crea DENTRO il context manager.
    proceed = True
    if AMD_ENABLED:
        proceed = await _run_amd(ctx, session, state, phone, participant_identity, agent.lead_name, frase_apertura)
    else:
        # AMD spento (test A/B): compone normalmente e prosegue.
        logger.info("AMD disabilitato (A/B): compongo senza rilevamento segreteria")
        try:
            await _create_sip(ctx, phone, participant_identity, agent.lead_name)
            await asyncio.wait_for(
                ctx.wait_for_participant(identity=participant_identity), timeout=ANSWER_WAIT_S
            )
        except asyncio.TimeoutError:
            logger.warning("Nessuna risposta dal lead entro %ss", ANSWER_WAIT_S)
            state["ended_reason"] = "no_answer"
            await _hangup(ctx)
            return
        except Exception as e:  # noqa: BLE001
            logger.warning("Chiamata non connessa: %s", e)
            state["ended_reason"] = "no_answer"
            await _hangup(ctx)
            return

    if not proceed:
        return  # segreteria/mai connessa: _run_amd ha già impostato ended_reason e riagganciato

    # 8) PROCEED: c'è una persona. Solo ORA avvia la musichetta d'attesa "thinking"
    #    (canale audio separato; parte da sola quando l'agente elabora un tool).
    try:
        background_audio = BackgroundAudioPlayer(thinking_sound=THINKING_SOUND)
        await background_audio.start(room=ctx.room, agent_session=session)
    except Exception as e:  # noqa: BLE001
        logger.warning("musichetta d'attesa non avviata: %s", e)

    # Watchdog: silenzio totale, durata massima, e "stuck" (rete anti-blocco da barge-in).
    async def _silence_watchdog() -> None:
        while not state["closing"]:
            await asyncio.sleep(5)
            if state["closing"]:
                return
            if loop.time() - state["last_activity"] > SILENCE_TIMEOUT_S:
                logger.info("Silence timeout (%ss) → hangup", SILENCE_TIMEOUT_S)
                _trigger_close("silence_timeout")
                return

    async def _max_duration_watchdog() -> None:
        await asyncio.sleep(MAX_DURATION_S)
        logger.info("Max duration (%ss) → hangup", MAX_DURATION_S)
        _trigger_close("max_duration")

    async def _stuck_watchdog() -> None:
        # Rete di sicurezza per il blocco da barge-in (portata da Boss): parte a contare SOLO
        # quando il cliente ha appena smesso di parlare e l'agente è fermo (non thinking/speaking).
        # In una chiamata sana l'agente reagisce entro 1-2s, quindi non scatta per errore.
        waiting_since = None
        prev_user_speaking = False
        while not state["closing"]:
            await asyncio.sleep(0.5)
            if state["closing"]:
                return
            now = loop.time()
            agent_busy = session.agent_state in ("thinking", "speaking")
            user_speaking = session.user_state == "speaking"
            if agent_busy or user_speaking:
                waiting_since = None
            else:
                if prev_user_speaking:
                    waiting_since = now
                if waiting_since is not None and (now - waiting_since) >= STUCK_TIMEOUT_S:
                    logger.warning(
                        "[STUCK] nessuna risposta dopo input cliente da %.0fs → chiusura",
                        STUCK_TIMEOUT_S,
                    )
                    _trigger_close("stuck")
                    return
            prev_user_speaking = user_speaking

    sw = asyncio.create_task(_silence_watchdog(), name="silence_watchdog")
    mw = asyncio.create_task(_max_duration_watchdog(), name="max_duration_watchdog")
    kw = asyncio.create_task(_stuck_watchdog(), name="stuck_watchdog")
    # Cancella TUTTI i task allo shutdown (watchdog + eventuale empty-turn pendente).
    # NB: _send_eoc resta il PRIMO shutdown callback registrato → l'EOC parte comunque.
    async def _cancel_watchdogs() -> None:
        for t in (sw, mw, kw):
            if not t.done():
                t.cancel()
    ctx.add_shutdown_callback(_cancel_watchdogs)
    ctx.add_shutdown_callback(lambda _=None: asyncio.ensure_future(_cancel_empty_turn()))

    # La rete anti-mutezza (empty-turn) si attiva solo da qui: niente reprompt durante l'AMD.
    state["conversation_started"] = True
    _touch()
    # 9) Il barge-in ha già detto la frase di apertura.
    #    Ora l'agente fa la domanda di scoperta dalla sessione principale (STT/LLM attivi).
    logger.info("Apertura barge-in completata — avvio domanda di scoperta")
    await session.generate_reply(
        instructions=(
            "Hai appena detto la frase di presentazione iniziale. "
            "Ora fai UNA sola domanda di scoperta, brevissima e naturale: "
            "chiedi se ha già un climatizzatore o come se la cava con il caldo d'estate. "
            "Una frase sola, poi aspetta la risposta del cliente."
        )
    )




async def _run_amd(
    ctx: JobContext,
    session: AgentSession,
    state: dict,
    phone: str,
    participant_identity: str,
    lead_name: str,
    frase_apertura: str = "",
) -> bool:
    """Esegue il rilevamento segreteria. Ritorna True se bisogna proseguire la
    conversazione (umano/incerto), False se ha già riagganciato (segreteria / mai connessa).

    Fail-safe: qualsiasi errore dell'AMD → si PROSEGUE come umano (mai riagganciare a una
    persona per colpa di un bug dell'AMD)."""

    # Focalizza l'audio della sessione sul callee prima che parta l'AMD, così non
    # entrano frame di altri partecipanti nella pipeline AMD (come nell'esempio ufficiale).
    try:
        if session.room_io:
            session.room_io.set_participant(participant_identity)
    except Exception as e:  # noqa: BLE001
        logger.debug("set_participant non applicato: %s", e)

    detection_options = {
        "human_speech_threshold": AMD_HUMAN_SPEECH_THRESHOLD_S,
        "human_silence_threshold": AMD_HUMAN_SILENCE_THRESHOLD_S,
        "machine_silence_threshold": AMD_MACHINE_SILENCE_THRESHOLD_S,
        "no_speech_threshold": AMD_NO_SPEECH_THRESHOLD_S,
        "timeout": AMD_TIMEOUT_S,
        "prompt": AMD_IT_PROMPT,
    }

    try:
        async with AMD(
            session,
            participant_identity=participant_identity,
            stt=deepgram.STT(model=DEEPGRAM_MODEL, language=DEEPGRAM_LANGUAGE, numerals=True),
            llm=openai.LLM(model=AMD_LLM_MODEL),
            ivr_detection=False,
            detection_options=detection_options,
            suppress_compatibility_warning=True,  # nova-3 e gpt-4.1-mini sono nel set valutato
        ) as detector:
            # La chiamata SIP si crea DENTRO il with, così l'AMD non perde l'audio iniziale.
            # In parallelo avviamo subito il pre-fetch TTS: durante il ringing l'audio viene
            # generato, così quando la persona risponde è già pronto → latenza ~0ms.
            try:
                await _create_sip(ctx, phone, participant_identity, lead_name)
            except Exception as e:  # noqa: BLE001
                logger.warning("create_sip_participant fallita (no_answer): %s", e)
                state["ended_reason"] = "no_answer"
                await _hangup(ctx)
                return False

            try:
                await asyncio.wait_for(
                    ctx.wait_for_participant(identity=participant_identity),
                    timeout=max(AMD_TIMEOUT_S, ANSWER_WAIT_S),
                )
            except asyncio.TimeoutError:
                logger.warning("Lead mai connesso entro il timeout → no_answer")
                state["ended_reason"] = "no_answer"
                await _hangup(ctx)
                return False

            # ── BARGE-IN: parla subito in parallelo con AMD ──
            # Frase brevissima = TTS pronto in <1s = latenza quasi zero.
            say_task: asyncio.Task | None = None
            if frase_apertura:
                async def _say_opening():
                    try:
                        await session.say(frase_apertura, allow_interruptions=False)
                        logger.info("BARGE-IN completato")
                    except Exception as _e:  # noqa: BLE001
                        logger.debug("say() barge-in interrotto: %s", _e)
                say_task = asyncio.create_task(_say_opening(), name="say_opening")
                logger.info("BARGE-IN avviato in parallelo con AMD")

            result = await detector.execute()

        category = getattr(result, "category", None)
        transcript = getattr(result, "transcript", "") or ""
        state["amd_category"] = category
        state["amd_transcript"] = transcript
        logger.info("AMD result: category=%s transcript=%r", category, transcript)

    except Exception as e:  # noqa: BLE001
        # Fail-safe: nel dubbio è una persona. Procedi con la conversazione.
        logger.exception("AMD ha sollevato un'eccezione: procedo come umano (%s)", e)
        state["amd_category"] = "uncertain"
        return True

    # ── Diramazione ──
    if category in ("human", "uncertain") or (category == "machine-ivr" and not AMD_IVR_HANGUP):
        logger.info("AMD → umano/incerto: proseguo con l'apertura a freddo")
        return True

    # machine-vm / machine-unavailable / (machine-ivr se AMD_IVR_HANGUP):
    # Cancella il barge-in se ancora in corso, poi riaggancia senza messaggi.
    if say_task and not say_task.done():
        say_task.cancel()
        logger.info("AMD → segreteria: barge-in cancellato")
    logger.info("AMD → segreteria (%s): riaggancio senza messaggi", category)
    state["ended_reason"] = "voicemail"
    await _hangup(ctx)
    return False


if __name__ == "__main__":
    cli.run_app(
        WorkerOptions(
            entrypoint_fnc=entrypoint,
            prewarm_fnc=prewarm,
            agent_name=AGENT_NAME,
        )
    )
