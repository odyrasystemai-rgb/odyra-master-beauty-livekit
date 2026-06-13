"""
Eva — Agente vocale LiveKit OUTBOUND (beauty/estetica), multi-tenant.

Worker self-hosted (Railway) che si connette a LiveKit Cloud per media/SIP.
I dati del tenant + del lead arrivano a RUNTIME nei metadata della room
(il pacchetto `variableValues` costruito dal nodo n8n "Chiamata Voce v2"),
NON da lookup su DB.

agent_name = "eva-outbound"  (deve combaciare col nodo n8n e col dispatcher).

Fix applicati vs versione originale:
- AMD_LLM_MODEL: passato come openai.LLM(model=...) non come stringa grezza.
- BARGE-IN: frase brevissima + generate_reply in parallelo con AMD → latenza ~1s.
- Cancellazione barge-in su segreteria rilevata.
- Rimosso generate_reply finale (sostituito dal barge-in).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import aiohttp
from dotenv import load_dotenv

from livekit import api
from livekit.agents import (
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

try:
    from livekit.agents import AMD  # type: ignore
    _AMD_AVAILABLE = True
except ImportError:
    AMD = None  # type: ignore
    _AMD_AVAILABLE = False

load_dotenv()

logger = logging.getLogger("eva")
logging.basicConfig(level=logging.INFO)

# ───────────────────────── Config ─────────────────────────

AGENT_NAME = "eva-outbound"

LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4.1-mini")

CARTESIA_VOICE = os.getenv("CARTESIA_VOICE", "36d94908-c5b9-4014-b521-e69aee5bead0")
CARTESIA_MODEL = os.getenv("CARTESIA_MODEL", "sonic-3") or "sonic-3"

KEYTERMS = [
    "chiocciola", "punto", "virgola", "trattino", "trattino basso", "underscore",
    "gmail", "yahoo", "hotmail", "outlook", "libero", "alice", "tim", "virgilio",
    "icloud", "protonmail", "email", "posta elettronica", "indirizzo mail",
    "prefisso", "cellulare", "numero di telefono",
    "zero", "uno", "due", "tre", "quattro", "cinque", "sei", "sette", "otto", "nove",
]

SIP_OUTBOUND_TRUNK_ID = os.getenv("SIP_OUTBOUND_TRUNK_ID", "ST_Yuv6LbXJTLno")
SIP_FROM_NUMBER = os.getenv("SIP_FROM_NUMBER", "+390230329429")

CORE_BASE = os.getenv(
    "CORE_BASE_URL",
    "https://primary-production-eb20.up.railway.app/webhook",
)
RAG_URL = os.getenv(
    "RAG_URL",
    "https://rag-production-6ab5.up.railway.app/",
)

EOC_WEBHOOK_URL = os.getenv(
    "EOC_WEBHOOK_URL",
    "https://primary-production-eb20.up.railway.app/webhook/vapi-end-of-call",
)

SILENCE_TIMEOUT_S = float(os.getenv("SILENCE_TIMEOUT_S", "191"))
COST_PER_MINUTE_EUR = float(os.getenv("EOC_COST_PER_MINUTE_EUR", "0") or 0)

AMD_ENABLED = os.getenv("AMD_ENABLED", "true").lower() in ("1", "true", "yes", "on")
AMD_LLM_MODEL = os.getenv("AMD_LLM_MODEL", "").strip() or None
AMD_IVR_HANGUP = os.getenv("AMD_IVR_HANGUP", "true").lower() in ("1", "true", "yes", "on")


def _read_amd_detection_options() -> dict:
    opts: dict = {}
    for key, env in (
        ("human_speech_threshold", "AMD_HUMAN_SPEECH_THRESHOLD_S"),
        ("human_silence_threshold", "AMD_HUMAN_SILENCE_THRESHOLD_S"),
        ("machine_silence_threshold", "AMD_MACHINE_SILENCE_THRESHOLD_S"),
        ("no_speech_threshold", "AMD_NO_SPEECH_THRESHOLD_S"),
        ("timeout", "AMD_TIMEOUT_S"),
    ):
        raw = os.getenv(env, "").strip()
        if not raw:
            continue
        try:
            opts[key] = float(raw)
        except ValueError:
            logger.warning("ENV %s non numerica (%r): ignorata", env, raw)
    return opts


AMD_DETECTION_OPTIONS = _read_amd_detection_options()

AMD_LEAVE_VOICEMAIL = os.getenv("AMD_LEAVE_VOICEMAIL", "false").lower() in ("1", "true", "yes", "on")
AMD_VOICEMAIL_INSTRUCTIONS = os.getenv(
    "AMD_VOICEMAIL_INSTRUCTIONS",
    "Hai raggiunto una segreteria telefonica. Lascia un messaggio MOLTO breve in "
    "italiano: presentati col nome del centro, di' che richiamerai a breve, e saluta "
    "con garbo. Massimo due frasi. Non lasciare numeri né dettagli.",
)

ROME = ZoneInfo("Europe/Rome")
ISO_OFFSET = "+01:00"

HERE = os.path.dirname(os.path.abspath(__file__))
PROMPT_PATH = os.path.join(HERE, "EVA_system_prompt.txt")

PLACEHOLDER_RE = re.compile(r"\{\{\s*([^}]+?)\s*\}\}")
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=20)

FILLER_KNOWLEDGE = "Mi lasci un istante, le verifico subito questa informazione."
FILLER_DISPONIBILITA = "Un istante, controllo l'agenda e le verifico la disponibilità."
FILLER_PRENOTAZIONE = "Resti pure in linea, le sto riservando l'appuntamento."
FILLER_SPOSTA = "Un istante, le sto spostando l'appuntamento."
FILLER_CANCELLA = "Mi lasci un istante, controllo subito i suoi appuntamenti."
FILLER_CUSTOMER = "Un istante, la cerco subito nei nostri registri."

FILLER_DELAY_S = 0.7

THINKING_SOUND = AudioConfig(
    BuiltinAudioClip.HOLD_MUSIC, volume=0.4, fade_in=0.3, fade_out=0.5
)


# ───────────────────────── Helpers ─────────────────────────

def _load_prompt_template() -> str:
    with open(PROMPT_PATH, "r", encoding="utf-8") as f:
        return f.read()


def fill_prompt(template: str, md: dict) -> str:
    return PLACEHOLDER_RE.sub(lambda m: str(md.get(m.group(1).strip(), "")), template)


def _now() -> datetime:
    return datetime.now(ROME)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + ISO_OFFSET


def _day_bounds(any_iso_or_date: str) -> tuple[str, str]:
    day = datetime.fromisoformat(any_iso_or_date[:10]).date()
    start = datetime(day.year, day.month, day.day, 0, 0, 0)
    end = datetime(day.year, day.month, day.day, 23, 59, 59)
    return _iso(start), _iso(end)


async def _post_json(url: str, payload: dict) -> str:
    try:
        async with aiohttp.ClientSession(timeout=HTTP_TIMEOUT) as s:
            async with s.post(url, json=payload) as resp:
                text = await resp.text()
                logger.info("POST %s -> %s", url, resp.status)
                return text
    except Exception as e:  # noqa: BLE001
        logger.exception("POST %s failed", url)
        return json.dumps({"error": str(e)})


def _extract_results(text: str) -> str:
    try:
        data = json.loads(text)
        results = data.get("results") or (data.get("data") or {}).get("results")
        if results:
            return json.dumps(results[0].get("result", results[0]), ensure_ascii=False)
    except Exception:  # noqa: BLE001
        pass
    return text


# ───────────────────────── Agent ─────────────────────────

class EvaAgent(Agent):
    def __init__(self, instructions: str, md: dict) -> None:
        super().__init__(instructions=instructions)
        self.md = md
        self._filler_handle = None
        self._filler_armed = True

    async def _fill_then(self, context: RunContext, phrase: str, coro):
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

    @property
    def tenant_id(self) -> str:
        return str(self.md.get("tenant_id", ""))

    @property
    def lead_phone(self) -> str:
        return str(self.md.get("lead_phone", ""))

    @property
    def lead_name(self) -> str:
        return str(self.md.get("lead_name", ""))

    @property
    def lead_email(self) -> str:
        return str(self.md.get("lead_email", ""))

    @function_tool()
    async def knowledge_query(self, context: RunContext, query: str) -> str:
        """Rispondi a domande su trattamenti, prodotti, risultati, prezzi, durate,
        sedi, promo del centro. `query` = la frase completa della cliente (mai una sola parola)."""
        return await self._fill_then(
            context, FILLER_KNOWLEDGE,
            _post_json(RAG_URL, {"id": self.tenant_id, "query": query}))

    @function_tool()
    async def get_data_oggi(self, context: RunContext) -> str:
        """Restituisce la data odierna e di domani (Europe/Rome). Usalo SOLO quando la
        cliente menziona un giorno relativo ("domani", "venerdì", "questa settimana")."""
        now = _now()
        tomorrow = now + timedelta(days=1)
        giorni = ["lunedì", "martedì", "mercoledì", "giovedì", "venerdì", "sabato", "domenica"]
        return json.dumps(
            {
                "oggi": now.strftime("%Y-%m-%d"),
                "oggi_giorno": giorni[now.weekday()],
                "domani": tomorrow.strftime("%Y-%m-%d"),
                "domani_giorno": giorni[tomorrow.weekday()],
                "ora": now.strftime("%H:%M"),
            },
            ensure_ascii=False,
        )

    @function_tool()
    async def cerca_disponibilita(
        self,
        context: RunContext,
        date_from: str,
        date_to: str,
        duration_min: int,
        operator_slug: str = "no_preference",
    ) -> str:
        """Disponibilità per un GIORNO SINGOLO preciso (servizio singolo)."""
        df, dt = _day_bounds(date_from or date_to)
        payload = {
            "tenant_id": self.tenant_id,
            "duration_min": duration_min,
            "operator_slug": operator_slug,
            "date_from": df,
            "date_to": dt,
            "phone": self.lead_phone,
        }
        return _extract_results(await self._fill_then(
            context, FILLER_DISPONIBILITA,
            _post_json(f"{CORE_BASE}/core-cerca-disponibilita", payload)))

    @function_tool()
    async def controllo_disponibilita(
        self,
        context: RunContext,
        prefareDateTime: str,
        duration_min: int,
        operator_slug: str = "no_preference",
    ) -> str:
        """Verifica uno SLOT a ORA PRECISA (servizio singolo)."""
        payload = {
            "prefareDateTime": prefareDateTime,
            "duration_min": duration_min,
            "operator_slug": operator_slug,
            "tenant_id": self.tenant_id,
            "phone_number": self.lead_phone,
        }
        if self.lead_email:
            payload["email"] = self.lead_email
        return await self._fill_then(
            context, FILLER_DISPONIBILITA,
            _post_json(f"{CORE_BASE}/core-disponibilita-slot-specifico", payload))

    @function_tool()
    async def cerca_disponibilita_settimana(
        self,
        context: RunContext,
        duration_min: int,
        operator_slug: str = "no_preference",
    ) -> str:
        """Disponibilità della prossima settimana (servizio singolo)."""
        start = (_now() + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=7)
        payload = {
            "tenant_id": self.tenant_id,
            "duration_min": duration_min,
            "operator_slug": operator_slug,
            "date_from": _iso(start),
            "date_to": _iso(end),
            "phone": self.lead_phone,
        }
        return _extract_results(await self._fill_then(
            context, FILLER_DISPONIBILITA,
            _post_json(f"{CORE_BASE}/core-cerca-disponibilita", payload)))

    @function_tool()
    async def cerca_disponibilita_multi(
        self,
        context: RunContext,
        service_1_name: str,
        service_1_duration_min: int,
        service_1_operator_slug: str,
        service_2_name: str,
        service_2_duration_min: int,
        service_2_operator_slug: str,
        service_3_name: str = "",
        service_3_duration_min: int = 0,
        service_3_operator_slug: str = "",
    ) -> str:
        """Disponibilità per 2 o 3 servizi INSIEME."""
        services = [
            {"name": service_1_name, "duration_min": service_1_duration_min,
             "operator_slug": service_1_operator_slug, "operator_name": ""},
            {"name": service_2_name, "duration_min": service_2_duration_min,
             "operator_slug": service_2_operator_slug, "operator_name": ""},
        ]
        if service_3_name:
            services.append({"name": service_3_name, "duration_min": service_3_duration_min,
                             "operator_slug": service_3_operator_slug, "operator_name": ""})
        start = (_now() + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=7)
        payload = {
            "tenant_id": self.tenant_id,
            "date_from": _iso(start),
            "date_to": _iso(end),
            "phone": self.lead_phone,
            "services": services,
        }
        return _extract_results(
            await self._fill_then(
                context, FILLER_DISPONIBILITA,
                _post_json(f"{CORE_BASE}/core-cerca-disponibilita-multi", payload))
        )

    @function_tool()
    async def prenotazione_appuntamento(
        self,
        context: RunContext,
        prefareDateTime: str,
        service: str,
        duration_min: int,
        operator_slug: str = "no_preference",
    ) -> str:
        """Prenota un appuntamento (servizio singolo) DOPO conferma della cliente."""
        payload = {
            "prefareDateTime": prefareDateTime,
            "name": self.lead_name,
            "phone": self.lead_phone,
            "service": service,
            "duration_min": duration_min,
            "operator_slug": operator_slug,
            "tenant_id": self.tenant_id,
        }
        if self.lead_email:
            payload["email"] = self.lead_email
        return await self._fill_then(
            context, FILLER_PRENOTAZIONE,
            _post_json(f"{CORE_BASE}/core-prenotazione-appuntamento", payload))

    @function_tool()
    async def prenotazione_multi(
        self,
        context: RunContext,
        slot_1_service: str,
        slot_1_start_iso: str,
        slot_1_end_iso: str,
        slot_1_operator_slug: str,
        slot_1_operator_name: str,
        slot_1_odoo_partner_id: str,
        slot_1_odoo_user_id: str,
        slot_2_service: str,
        slot_2_start_iso: str,
        slot_2_end_iso: str,
        slot_2_operator_slug: str,
        slot_2_operator_name: str,
        slot_2_odoo_partner_id: str,
        slot_2_odoo_user_id: str,
        slot_3_service: str = "",
        slot_3_start_iso: str = "",
        slot_3_end_iso: str = "",
        slot_3_operator_slug: str = "",
        slot_3_operator_name: str = "",
        slot_3_odoo_partner_id: str = "",
        slot_3_odoo_user_id: str = "",
    ) -> str:
        """Prenota 2 o 3 servizi insieme DOPO conferma esplicita."""
        slots = [_slot(1, locals()), _slot(2, locals())]
        if slot_3_service:
            slots.append(_slot(3, locals()))
        payload = {
            "tenant_id": self.tenant_id,
            "name": self.lead_name,
            "phone": self.lead_phone,
            "slots": slots,
        }
        return await self._fill_then(
            context, FILLER_PRENOTAZIONE,
            _post_json(f"{CORE_BASE}/core-prenotazione-multi", payload))

    @function_tool()
    async def riprenotazione_appuntamento(
        self,
        context: RunContext,
        event_id: str,
        new_datetime: str,
        service: str = "",
        duration_min: int = 0,
        operator_slug: str = "no_preference",
        orario: str = "",
    ) -> str:
        """Sposta un singolo appuntamento."""
        payload = {
            "event_id ": event_id,
            "phone": self.lead_phone,
            "service": service,
            "duration_min": duration_min,
            "operator_slug": operator_slug,
            "orario": orario,
            "tenant_id": self.tenant_id,
            "new_datetime": new_datetime,
        }
        if self.lead_email:
            payload["email"] = self.lead_email
        return await self._fill_then(
            context, FILLER_SPOSTA,
            _post_json(f"{CORE_BASE}/core-spostamento-appuntamento", payload))

    @function_tool()
    async def spostamento_multi(
        self,
        context: RunContext,
        event_id_1: str,
        event_id_2: str,
        slot_1_service: str,
        slot_1_start_iso: str,
        slot_1_end_iso: str,
        slot_1_operator_slug: str,
        slot_1_operator_name: str,
        slot_1_odoo_partner_id: str,
        slot_1_odoo_user_id: str,
        slot_2_service: str,
        slot_2_start_iso: str,
        slot_2_end_iso: str,
        slot_2_operator_slug: str,
        slot_2_operator_name: str,
        slot_2_odoo_partner_id: str,
        slot_2_odoo_user_id: str,
        event_id_3: str = "",
        slot_3_service: str = "",
        slot_3_start_iso: str = "",
        slot_3_end_iso: str = "",
        slot_3_operator_slug: str = "",
        slot_3_operator_name: str = "",
        slot_3_odoo_partner_id: str = "",
        slot_3_odoo_user_id: str = "",
    ) -> str:
        """Sposta 2 o 3 appuntamenti multi-servizio."""
        event_ids = [event_id_1, event_id_2]
        slots = [_slot(1, locals()), _slot(2, locals())]
        if event_id_3:
            event_ids.append(event_id_3)
        if slot_3_service:
            slots.append(_slot(3, locals()))
        payload = {
            "tenant_id": self.tenant_id,
            "name": self.lead_name,
            "phone": self.lead_phone,
            "event_ids": event_ids,
            "slots": slots,
        }
        return await self._fill_then(
            context, FILLER_SPOSTA,
            _post_json(f"{CORE_BASE}/core-spostamento-multi", payload))

    @function_tool()
    async def cancellazione_appuntamento(self, context: RunContext, event_id: str) -> str:
        """Cancella un singolo appuntamento."""
        payload = {
            "event_id ": event_id,
            "phone": self.lead_phone,
            "tenant_id": self.tenant_id,
        }
        if self.lead_email:
            payload["email"] = self.lead_email
        return await self._fill_then(
            context, FILLER_CANCELLA,
            _post_json(f"{CORE_BASE}/core-cancellazione-appuntamento", payload))

    @function_tool()
    async def cancellazione_multi(
        self,
        context: RunContext,
        event_id_1: str,
        event_id_2: str,
        event_id_3: str = "",
    ) -> str:
        """Cancella 2 o 3 appuntamenti."""
        event_ids = [event_id_1, event_id_2]
        if event_id_3:
            event_ids.append(event_id_3)
        payload = {"tenant_id": self.tenant_id, "event_ids": event_ids}
        return await self._fill_then(
            context, FILLER_CANCELLA,
            _post_json(f"{CORE_BASE}/core-cancellazione-multi", payload))

    @function_tool()
    async def customer_verification(self, context: RunContext) -> str:
        """Verifica se la cliente è già presente nel gestionale."""
        payload = {"phone": self.lead_phone, "tenant_id": self.tenant_id}
        return await self._fill_then(
            context, FILLER_CUSTOMER,
            _post_json(f"{CORE_BASE}/core-customer-verification", payload))

    @function_tool()
    async def trasferisci_operatore(self, context: RunContext, motivo: str) -> str:
        """Trasferisci la chiamata a un operatore umano."""
        resolver = await _post_json(
            f"{CORE_BASE}/core-handoff-resolver",
            {"motivo": motivo, "tenant_id": self.tenant_id},
        )
        handoff_number = str(self.md.get("handoff_number", "")).strip()
        if not handoff_number:
            return json.dumps(
                {"resolver": resolver, "transfer": "skipped",
                 "reason": "handoff_number assente nei metadata"},
                ensure_ascii=False,
            )
        try:
            await _transfer_call_to(self._job_ctx, handoff_number)
            return json.dumps({"resolver": resolver, "transfer": "ok"}, ensure_ascii=False)
        except Exception as e:  # noqa: BLE001
            logger.exception("SIP transfer failed")
            return json.dumps(
                {"resolver": resolver, "transfer": "failed", "error": str(e)},
                ensure_ascii=False,
            )

    _job_ctx: JobContext | None = None


def _slot(n: int, scope: dict) -> dict:
    return {
        "service": scope[f"slot_{n}_service"],
        "start_iso": scope[f"slot_{n}_start_iso"],
        "end_iso": scope[f"slot_{n}_end_iso"],
        "operator_slug": scope[f"slot_{n}_operator_slug"],
        "operator_name": scope[f"slot_{n}_operator_name"],
        "odoo_partner_id": scope[f"slot_{n}_odoo_partner_id"],
        "odoo_user_id": scope[f"slot_{n}_odoo_user_id"],
    }


async def _transfer_call_to(ctx: JobContext, number: str) -> None:
    sip_to = number if number.startswith("tel:") or number.startswith("sip:") else f"tel:{number}"
    sip_identity = None
    for ident, p in ctx.room.remote_participants.items():
        if str(getattr(p, "kind", "")).upper().endswith("SIP") or ident.startswith("sip-"):
            sip_identity = ident
            break
    if not sip_identity:
        raise RuntimeError("Nessun partecipante SIP da trasferire")
    await ctx.api.sip.transfer_sip_participant(
        api.TransferSIPParticipantRequest(
            participant_identity=sip_identity,
            room_name=ctx.room.name,
            transfer_to=sip_to,
            play_dialtone=True,
        )
    )


def build_tts():
    return cartesia.TTS(
        model=CARTESIA_MODEL,
        voice=CARTESIA_VOICE,
        language="it",
        speed=1.1,
        volume=1.2,
        word_timestamps=False,
    )


# ───────────────────────── Worker lifecycle ─────────────────────────

def prewarm(proc: JobProcess) -> None:
    proc.userdata["vad"] = silero.VAD.load()


async def entrypoint(ctx: JobContext) -> None:
    try:
        md = json.loads(ctx.job.metadata or "{}")
    except json.JSONDecodeError:
        logger.error("Metadata non è JSON valido; uso dict vuoto")
        md = {}
    logger.info("Eva job per tenant=%s lead=%s", md.get("tenant_id"), md.get("lead_id"))

    system_prompt = fill_prompt(_load_prompt_template(), md)

    await ctx.connect()

    vad = ctx.proc.userdata.get("vad") or silero.VAD.load()
    session = AgentSession(
        stt=deepgram.STT(model="nova-3-general", language="it", numerals=True, keyterm=KEYTERMS),
        llm=openai.LLM(model=LLM_MODEL, temperature=0.3, parallel_tool_calls=False),
        tts=build_tts(),
        vad=vad,
        turn_detection=MultilingualModel(),
        min_endpointing_delay=0.6,
        max_endpointing_delay=2.0,
    )

    agent = EvaAgent(instructions=system_prompt, md=md)
    agent._job_ctx = ctx

    loop = asyncio.get_running_loop()
    state = {"ended_reason": "completed", "start": loop.time(), "last_activity": loop.time()}

    def _touch(*_args) -> None:
        state["last_activity"] = loop.time()

    session.on("user_state_changed", _touch)
    session.on("agent_state_changed", _touch)

    def _on_user_text(ev) -> None:
        _touch()
        if getattr(ev, "is_final", False):
            agent._filler_armed = True

    session.on("user_input_transcribed", _on_user_text)

    def _on_disconnect(participant) -> None:
        ident = getattr(participant, "identity", "")
        if ident.startswith("sip-") or str(getattr(participant, "kind", "")).upper().endswith("SIP"):
            state["ended_reason"] = "customer_hangup"
            asyncio.create_task(_hangup(ctx))

    ctx.room.on("participant_disconnected", _on_disconnect)

    async def _send_eoc() -> None:
        duration = max(0, int(loop.time() - state["start"]))
        messages, transcript = _build_transcript(session)
        payload = {
            "lead_id": md.get("lead_id"),
            "tenant_id": md.get("tenant_id"),
            "duration_seconds": duration,
            "ended_reason": state["ended_reason"],
            "costEur": round(duration / 60.0 * COST_PER_MINUTE_EUR, 4),
            "transcript": transcript,
            "messages": messages,
            "lead_name": md.get("lead_name"),
            "lead_phone": md.get("lead_phone"),
            "attempt_number": md.get("attempt_number"),
            "agent": AGENT_NAME,
            "amd_category": state.get("amd_category"),
        }
        logger.info("EOC POST (reason=%s, dur=%ss)", state["ended_reason"], duration)
        await _post_json(EOC_WEBHOOK_URL, payload)

    ctx.add_shutdown_callback(_send_eoc)

    await session.start(agent=agent, room=ctx.room)

    try:
        background_audio = BackgroundAudioPlayer(thinking_sound=THINKING_SOUND)
        await background_audio.start(room=ctx.room, agent_session=session)
    except Exception as e:  # noqa: BLE001
        logger.warning("musichetta d'attesa non avviata: %s", e)

    # ── Costruisce la frase di apertura PRIMA dell'AMD (barge-in) ──
    lead_name_barge = str(md.get("lead_name", "")).strip()
    nome_parte_barge = f", {lead_name_barge}" if lead_name_barge else ""
    business_name_barge = str(md.get("business_name", "il nostro centro")).strip()
    frase_apertura_barge = f"Buongiorno{nome_parte_barge}, sono la consulente di {business_name_barge}!"

    if _AMD_AVAILABLE and AMD_ENABLED:
        amd_kwargs: dict = {"ivr_detection": False, "suppress_compatibility_warning": True}
        if AMD_LLM_MODEL:
            # FIX: openai.LLM(model=...) non stringa grezza
            amd_kwargs["llm"] = openai.LLM(model=AMD_LLM_MODEL)
        if AMD_DETECTION_OPTIONS:
            amd_kwargs["detection_options"] = AMD_DETECTION_OPTIONS

        say_task = None

        async with AMD(session, **amd_kwargs) as detector:
            try:
                await asyncio.wait_for(ctx.wait_for_participant(), timeout=90)
            except asyncio.TimeoutError:
                logger.warning("Nessuna risposta dal lead entro il timeout")
                state["ended_reason"] = "no_answer"
                await _hangup(ctx)
                return

            # BARGE-IN: frase brevissima + generate_reply in parallelo con AMD
            async def _say_opening_eva():
                try:
                    await session.say(frase_apertura_barge, allow_interruptions=False)
                    logger.info("BARGE-IN Eva completato")
                    await session.generate_reply(
                        instructions=(
                            "Hai appena detto la presentazione. "
                            "Prosegui con STEP 1 del prompt: "
                            "contesto della chiamata e domanda di apertura. "
                            "Una sola frase, poi aspetta la risposta."
                        )
                    )
                except Exception as _e:  # noqa: BLE001
                    logger.debug("barge-in Eva interrotto: %s", _e)

            say_task = asyncio.create_task(_say_opening_eva(), name="say_opening_eva")
            logger.info("BARGE-IN Eva avviato in parallelo con AMD")

            try:
                result = await detector.execute()
                category = getattr(result, "category", "uncertain")
            except Exception as e:  # noqa: BLE001
                logger.warning("AMD fallita (%s) → procedo come umano", e)
                category = "uncertain"
            state["amd_category"] = category
            logger.info("AMD category=%s", category)

            hangup_categories = {"machine-vm", "machine-unavailable"}
            if AMD_IVR_HANGUP:
                hangup_categories.add("machine-ivr")

            if category in hangup_categories:
                if say_task and not say_task.done():
                    say_task.cancel()
                    logger.info("AMD → segreteria: barge-in Eva cancellato")
                if category == "machine-vm" and AMD_LEAVE_VOICEMAIL:
                    try:
                        await session.generate_reply(instructions=AMD_VOICEMAIL_INSTRUCTIONS)
                    except Exception:  # noqa: BLE001
                        logger.exception("messaggio in segreteria non riuscito")
                state["ended_reason"] = {
                    "machine-vm": "voicemail",
                    "machine-unavailable": "machine_unavailable",
                    "machine-ivr": "machine_ivr",
                }.get(category, "machine")
                await _hangup(ctx)
                return
            # human/uncertain → conversazione già avviata dal barge-in

    else:
        if AMD_ENABLED and not _AMD_AVAILABLE:
            logger.warning("AMD richiesto ma non disponibile → flusso senza rilevamento segreteria")
        try:
            await asyncio.wait_for(ctx.wait_for_participant(), timeout=90)
        except asyncio.TimeoutError:
            logger.warning("Nessuna risposta dal lead entro il timeout")
            state["ended_reason"] = "no_answer"
            await _hangup(ctx)
            return
        # Fallback senza AMD: apertura diretta
        try:
            await session.say(frase_apertura_barge, allow_interruptions=False)
            await session.generate_reply(
                instructions=(
                    "Hai appena detto la presentazione. "
                    "Prosegui con STEP 1 del prompt: "
                    "contesto e domanda di apertura. Una frase, poi aspetta."
                )
            )
        except Exception as e:  # noqa: BLE001
            logger.debug("apertura fallback Eva: %s", e)

    # Watchdog silenzio
    async def _silence_watchdog() -> None:
        while True:
            await asyncio.sleep(5)
            if loop.time() - state["last_activity"] > SILENCE_TIMEOUT_S:
                logger.info("Silence timeout (%ss) → hangup", SILENCE_TIMEOUT_S)
                state["ended_reason"] = "silence_timeout"
                await _hangup(ctx)
                return

    watchdog_task = asyncio.create_task(_silence_watchdog())
    ctx.add_shutdown_callback(lambda _=None: watchdog_task.cancel() if not watchdog_task.done() else None)

    _touch()


async def _hangup(ctx: JobContext) -> None:
    try:
        await ctx.api.room.delete_room(api.DeleteRoomRequest(room=ctx.room.name))
    except Exception:  # noqa: BLE001
        logger.exception("delete_room fallita")


def _build_transcript(session: AgentSession) -> tuple[list[dict], str]:
    messages: list[dict] = []
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
            if text:
                messages.append({"role": role, "content": text})
    except Exception:  # noqa: BLE001
        logger.exception("Impossibile costruire la trascrizione")
    transcript = "\n".join(f"{m['role']}: {m['content']}" for m in messages)
    return messages, transcript


if __name__ == "__main__":
    cli.run_app(
        WorkerOptions(
            entrypoint_fnc=entrypoint,
            prewarm_fnc=prewarm,
            agent_name=AGENT_NAME,
        )
    )
