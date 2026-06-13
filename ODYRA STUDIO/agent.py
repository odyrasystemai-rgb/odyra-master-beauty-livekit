"""
Eva — Agente vocale LiveKit OUTBOUND (beauty/estetica), multi-tenant.

Worker self-hosted (Railway) che si connette a LiveKit Cloud per media/SIP.
I dati del tenant + del lead arrivano a RUNTIME nei metadata della room
(il pacchetto `variableValues` costruito dal nodo n8n "Chiamata Voce v2"),
NON da lookup su DB.

agent_name = "eva-outbound"  (deve combaciare col nodo n8n e col dispatcher).

Costruito da zero con i pattern standard di livekit-agents 1.5.x.
Non dipende dall'agente "Boss".
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

# AMD (Answering Machine Detection) — disponibile nelle versioni recenti di
# livekit-agents. Import difensivo: se la versione installata non lo include,
# il worker NON crasha e ricade sul flusso originale senza rilevamento.
try:
    from livekit.agents import AMD  # type: ignore
    _AMD_AVAILABLE = True
except ImportError:
    AMD = None  # type: ignore
    _AMD_AVAILABLE = False

load_dotenv()

logger = logging.getLogger("eva")
logging.basicConfig(level=logging.INFO)

# ───────────────────────── Config (vedi EVA_build_brief.md) ─────────────────────────

AGENT_NAME = "eva-outbound"

# LLM / TTS / STT
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4.1-mini")

# Cartesia: voice e model dal brief. Se il plugin non accetta "sonic-3.5",
# si usa l'ultima `sonic` disponibile (override via env CARTESIA_MODEL).
CARTESIA_VOICE = os.getenv("CARTESIA_VOICE", "36d94908-c5b9-4014-b521-e69aee5bead0")
CARTESIA_MODEL = os.getenv("CARTESIA_MODEL", "sonic-3") or "sonic-3"

# Deepgram nova-3, italiano, numerals on, con keyterm list (in fondo al brief).
KEYTERMS = [
    "chiocciola", "punto", "virgola", "trattino", "trattino basso", "underscore",
    "gmail", "yahoo", "hotmail", "outlook", "libero", "alice", "tim", "virgilio",
    "icloud", "protonmail", "email", "posta elettronica", "indirizzo mail",
    "prefisso", "cellulare", "numero di telefono",
    "zero", "uno", "due", "tre", "quattro", "cinque", "sei", "sette", "otto", "nove",
]

# SIP outbound (Eva). NON usare il trunk di Boss.
SIP_OUTBOUND_TRUNK_ID = os.getenv("SIP_OUTBOUND_TRUNK_ID", "ST_Yuv6LbXJTLno")
SIP_FROM_NUMBER = os.getenv("SIP_FROM_NUMBER", "+390230329429")

# Webhook base URLs (override via env; default = host di produzione attuali).
CORE_BASE = os.getenv(
    "CORE_BASE_URL",
    "https://primary-production-eb20.up.railway.app/webhook",
)  # tool gestionali/booking (odyra-studio-api)
RAG_URL = os.getenv(
    "RAG_URL",
    "https://rag-production-6ab5.up.railway.app/",
)  # knowledge (host DIVERSO)

# End-of-call webhook (lo stesso che usava l'agente Beauty su Vapi).
EOC_WEBHOOK_URL = os.getenv(
    "EOC_WEBHOOK_URL",
    "https://primary-production-eb20.up.railway.app/webhook/vapi-end-of-call",
)

# Silence auto-hangup (valore originale Beauty).
SILENCE_TIMEOUT_S = float(os.getenv("SILENCE_TIMEOUT_S", "191"))

# Stima costo per minuto (EUR) per il payload EOC; il calcolo "vero" lo fa n8n.
COST_PER_MINUTE_EUR = float(os.getenv("EOC_COST_PER_MINUTE_EUR", "0") or 0)

# ── AMD (Answering Machine Detection) ──
# Stesso set di ENV del worker Boss, così le due immagini si configurano allo stesso
# modo (puoi anche promuoverle a Shared Variables Railway e condividerle tra i servizi).
# Kill-switch: AMD_ENABLED=false → comportamento originale (nessun rilevamento).
AMD_ENABLED = os.getenv("AMD_ENABLED", "true").lower() in ("1", "true", "yes", "on")
# Modello per la classificazione (LiveKit Inference model ID, es. "openai/gpt-4.1-mini"
# o "google/gemini-3.1-flash-lite"). Vuoto = default LiveKit (gemini-3.1-flash-lite).
AMD_LLM_MODEL = os.getenv("AMD_LLM_MODEL", "").strip() or None
# machine-ivr → riagganciare? (true per outbound a freddo: l'IVR non è il lead).
AMD_IVR_HANGUP = os.getenv("AMD_IVR_HANGUP", "true").lower() in ("1", "true", "yes", "on")


def _read_amd_detection_options() -> dict:
    """Soglie di detection (secondi) dalle ENV. Solo quelle effettivamente impostate;
    le altre ricadono sui default della libreria (human_speech 2.5, human_silence 0.5,
    machine_silence 1.5, no_speech 10.0, timeout 20.0)."""
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

# Extra Eva-specifico (il worker Boss non lo usa): su machine-vm, lasciare un breve
# messaggio in segreteria prima di chiudere. Default OFF (per outbound a freddo conviene
# riprovare dopo). Se lo accendi, personalizza il testo con AMD_VOICEMAIL_INSTRUCTIONS.
AMD_LEAVE_VOICEMAIL = os.getenv("AMD_LEAVE_VOICEMAIL", "false").lower() in ("1", "true", "yes", "on")
AMD_VOICEMAIL_INSTRUCTIONS = os.getenv(
    "AMD_VOICEMAIL_INSTRUCTIONS",
    "Hai raggiunto una segreteria telefonica. Lascia un messaggio MOLTO breve in "
    "italiano: presentati col nome del centro, di' che richiamerai a breve, e saluta "
    "con garbo. Massimo due frasi. Non lasciare numeri né dettagli.",
)

# Date/ISO. Il brief richiede ESPLICITAMENTE offset +01:00 (come l'originale Vapi).
ROME = ZoneInfo("Europe/Rome")
ISO_OFFSET = "+01:00"

HERE = os.path.dirname(os.path.abspath(__file__))
PROMPT_PATH = os.path.join(HERE, "EVA_system_prompt.txt")

PLACEHOLDER_RE = re.compile(r"\{\{\s*([^}]+?)\s*\}\}")
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=20)

# ───────────────────────── Filler vocali + musichetta (pattern di Boss) ─────────────────────────
# FILLER PHRASES — pronunciate mentre un tool gira, per evitare il silenzio.
FILLER_KNOWLEDGE = "Mi lasci un istante, le verifico subito questa informazione."
FILLER_DISPONIBILITA = "Un istante, controllo l'agenda e le verifico la disponibilità."
FILLER_PRENOTAZIONE = "Resti pure in linea, le sto riservando l'appuntamento."
FILLER_SPOSTA = "Un istante, le sto spostando l'appuntamento."
FILLER_CANCELLA = "Mi lasci un istante, controllo subito i suoi appuntamenti."
FILLER_CUSTOMER = "Un istante, la cerco subito nei nostri registri."

# Soglia "filler intelligente" (stile Vapi request-response-delayed): la frase d'attesa
# parte SOLO se il tool supera questo tempo. Sotto soglia l'agente tace e la pausa breve
# è coperta dalla musichetta → niente "un istante" inutile sulle risposte rapide.
FILLER_DELAY_S = 0.7

# MUSICHETTA D'ATTESA (BackgroundAudioPlayer.thinking_sound): suono soft riprodotto SOLO
# mentre l'agente è in stato "thinking" (tool in corso), su un canale audio separato gestito
# dal framework → NON compete con la voce della risposta vera, quindi non può zittire l'agente.
THINKING_SOUND = AudioConfig(
    BuiltinAudioClip.HOLD_MUSIC, volume=0.4, fade_in=0.3, fade_out=0.5
)


# ───────────────────────── Helpers ─────────────────────────

def _load_prompt_template() -> str:
    with open(PROMPT_PATH, "r", encoding="utf-8") as f:
        return f.read()


def fill_prompt(template: str, md: dict) -> str:
    """Sostituisce ogni {{chiave}} con md["chiave"]; placeholder mancante → stringa vuota."""
    return PLACEHOLDER_RE.sub(lambda m: str(md.get(m.group(1).strip(), "")), template)


def _now() -> datetime:
    return datetime.now(ROME)


def _iso(dt: datetime) -> str:
    """Formatta in ISO 8601 con offset +01:00 (come da brief)."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + ISO_OFFSET


def _day_bounds(any_iso_or_date: str) -> tuple[str, str]:
    """Da una data/datetime ISO ricava (giorno 00:00, giorno 23:59) in ISO +01:00."""
    day = datetime.fromisoformat(any_iso_or_date[:10]).date()
    start = datetime(day.year, day.month, day.day, 0, 0, 0)
    end = datetime(day.year, day.month, day.day, 23, 59, 59)
    return _iso(start), _iso(end)


async def _post_json(url: str, payload: dict) -> str:
    """POST JSON, ritorna il testo della risposta (o un messaggio d'errore leggibile dal modello)."""
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
    """Per le cerche 'code': prova a estrarre data.results[0].result, altrimenti ritorna grezzo."""
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
    """Agente Eva. `tenant_id` e `phone` NON sono argomenti del modello: vengono
    iniettati dai metadata della room. `name` = lead_name dai metadata."""

    def __init__(self, instructions: str, md: dict) -> None:
        super().__init__(instructions=instructions)
        self.md = md
        self._filler_handle = None
        # Filler "armato": dice UNA sola frase d'attesa per turno utente (al primo tool).
        # I tool successivi nello stesso turno NON parlano (li copre la musichetta) → evita
        # l'accavallarsi di più say() che ruberebbero la voce alla risposta vera. Ri-armato
        # a ogni nuovo turno utente (vedi handler user_input_transcribed nell'entrypoint).
        self._filler_armed = True

    async def _fill_then(self, context: RunContext, phrase: str, coro):
        """Esegue il lavoro del tool (`coro`) e, SOLO se supera FILLER_DELAY_S, dice la
        frase d'attesa (max 1 per turno). Se il lavoro finisce prima della soglia il filler
        NON parte: sulle risposte rapide l'agente non dice "un istante" inutilmente — la
        pausa breve è già coperta dalla musichetta. Non aumenta mai i say() per turno."""
        filler_task = asyncio.create_task(self._delayed_filler(context, phrase))
        try:
            return await coro
        finally:
            filler_task.cancel()  # tool finito: se il filler non è ancora partito, non parte

    async def _delayed_filler(self, context: RunContext, phrase: str) -> None:
        """Attende la soglia; se non viene cancellato (tool ancora in corso) dice UNA frase.
        add_to_chat_ctx=False: solo voce, fuori dal contesto del modello."""
        try:
            await asyncio.sleep(FILLER_DELAY_S)
        except asyncio.CancelledError:
            return  # il tool è finito prima della soglia → niente filler
        try:
            if not self._filler_armed:
                return  # già detta una frase in questo turno
            if self._filler_handle is not None and not self._filler_handle.done():
                return
            self._filler_armed = False
            self._filler_handle = context.session.say(
                phrase, allow_interruptions=True, add_to_chat_ctx=False
            )
        except Exception as e:  # noqa: BLE001
            logger.debug("filler skipped: %s", e)

    # --- accessor metadata (mai esposti al modello) ---
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

    # ───────── A. Knowledge & utility ─────────

    @function_tool()
    async def knowledge_query(self, context: RunContext, query: str) -> str:
        """Rispondi a domande su trattamenti, prodotti, risultati, prezzi, durate,
        sedi, promo del centro. `query` = la frase completa della cliente (mai una sola parola)."""
        # NB: host RAG diverso e campo `id` (= tenant_id), non `tenant_id`.
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

    # ───────── B. Ricerca disponibilità ─────────

    @function_tool()
    async def cerca_disponibilita(
        self,
        context: RunContext,
        date_from: str,
        date_to: str,
        duration_min: int,
        operator_slug: str = "no_preference",
    ) -> str:
        """Disponibilità per un GIORNO SINGOLO preciso (servizio singolo).
        Passa la data del giorno richiesto in `date_from` (formato ISO/YYYY-MM-DD).
        Il sistema espande automaticamente il giorno a 00:00–23:59."""
        # Espande il giorno richiesto (derivato da date_from) a 00:00..23:59 +01:00.
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
        """Verifica uno SLOT a ORA PRECISA (servizio singolo). `prefareDateTime` in ISO 8601
        (YYYY-MM-DDTHH:MM:SS). Usalo quando la cliente indica un orario puntuale."""
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
        """Disponibilità della prossima settimana (servizio singolo), quando la cliente
        non ha preferenze di giorno. Calcola da domani (00:00) a +7 giorni."""
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
        """Disponibilità per 2 o 3 servizi INSIEME. Passa nome, durata e operator_slug
        di ogni servizio (il 3° è opzionale). NON usare la cerca singola per più servizi."""
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

    # ───────── C. Prenotazione ─────────

    @function_tool()
    async def prenotazione_appuntamento(
        self,
        context: RunContext,
        prefareDateTime: str,
        service: str,
        duration_min: int,
        operator_slug: str = "no_preference",
    ) -> str:
        """Prenota un appuntamento (servizio singolo) DOPO che la cliente ha confermato uno slot.
        `prefareDateTime` in ISO 8601. name e phone sono presi dai dati lead."""
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
        """Prenota 2 o 3 servizi insieme DOPO conferma esplicita di una combinazione.
        Passa gli slot ESATTAMENTE come restituiti dalla cerca disponibilità multi."""
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

    # ───────── D. Spostamento ─────────

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
        """Sposta un singolo appuntamento. `event_id` = id dell'appuntamento esistente,
        `new_datetime` in ISO 8601."""
        # ATTENZIONE: la chiave del webhook è "event_id " con uno SPAZIO finale (da preservare).
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
        """Sposta 2 o 3 appuntamenti multi-servizio. Passa gli event_id esistenti e i
        nuovi slot (come restituiti dalla cerca disponibilità multi)."""
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

    # ───────── E. Cancellazione ─────────

    @function_tool()
    async def cancellazione_appuntamento(self, context: RunContext, event_id: str) -> str:
        """Cancella un singolo appuntamento. `event_id` = id dell'appuntamento."""
        # ATTENZIONE: chiave "event_id " con SPAZIO finale (da preservare).
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
        """Cancella 2 o 3 appuntamenti. Passa gli event_id (il 3° è opzionale)."""
        event_ids = [event_id_1, event_id_2]
        if event_id_3:
            event_ids.append(event_id_3)
        payload = {"tenant_id": self.tenant_id, "event_ids": event_ids}
        return await self._fill_then(
            context, FILLER_CANCELLA,
            _post_json(f"{CORE_BASE}/core-cancellazione-multi", payload))

    # ───────── F. Verifica & handoff ─────────

    @function_tool()
    async def customer_verification(self, context: RunContext) -> str:
        """Verifica se la cliente è già presente nel gestionale (per telefono)."""
        payload = {"phone": self.lead_phone, "tenant_id": self.tenant_id}
        return await self._fill_then(
            context, FILLER_CUSTOMER,
            _post_json(f"{CORE_BASE}/core-customer-verification", payload))

    @function_tool()
    async def trasferisci_operatore(self, context: RunContext, motivo: str) -> str:
        """Trasferisci la chiamata a un operatore umano. Usalo SOLO se la cliente lo
        chiede esplicitamente. `motivo` = breve ragione del trasferimento."""
        # 1) Resolver: logga/risolve il numero lato n8n.
        resolver = await _post_json(
            f"{CORE_BASE}/core-handoff-resolver",
            {"motivo": motivo, "tenant_id": self.tenant_id},
        )
        # 2) Transfer SIP reale verso handoff_number (dai metadata), se presente.
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
            # TODO(transfer): se l'API di transfer non è disponibile/abilitata nel progetto,
            # il transfer vero va cablato dopo che il flusso base funziona. Per ora si chiude
            # con garbo informando la cliente.
            logger.exception("SIP transfer failed")
            return json.dumps(
                {"resolver": resolver, "transfer": "failed", "error": str(e)},
                ensure_ascii=False,
            )

    # riferimento al JobContext (impostato in entrypoint) per il transfer SIP
    _job_ctx: JobContext | None = None


def _slot(n: int, scope: dict) -> dict:
    """Assembla un dict slot dai parametri flat slot_N_* presenti in `scope` (locals())."""
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
    """Transfer SIP del partecipante (il lead) verso `number` via LiveKit Server API."""
    sip_to = number if number.startswith("tel:") or number.startswith("sip:") else f"tel:{number}"
    # Trova il partecipante SIP (il lead) nella room.
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


# ───────────────────────── TTS builder (controlli espressivi con fallback) ─────────────────────────

def build_tts():
    # Config allineata a quella provata di Boss. sonic-3 con speed/volume numerici
    # (parametri standard, NON i controlli sperimentali "fastest"/emotion).
    return cartesia.TTS(
        model=CARTESIA_MODEL,           # "sonic-3"
        voice=CARTESIA_VOICE,
        language="it",
        speed=1.1,
        volume=1.2,
    )


# ───────────────────────── Worker lifecycle ─────────────────────────

def prewarm(proc: JobProcess) -> None:
    # Carica il VAD una sola volta per processo (riusato dai job).
    proc.userdata["vad"] = silero.VAD.load()


async def entrypoint(ctx: JobContext) -> None:
    # 1) Metadata della room → variableValues
    try:
        md = json.loads(ctx.job.metadata or "{}")
    except json.JSONDecodeError:
        logger.error("Metadata non è JSON valido; uso dict vuoto")
        md = {}
    logger.info("Eva job per tenant=%s lead=%s", md.get("tenant_id"), md.get("lead_id"))

    # 2) System prompt: template + sostituzione {{placeholder}} dai metadata
    system_prompt = fill_prompt(_load_prompt_template(), md)

    await ctx.connect()

    # 3) Sessione (STT/LLM/TTS/VAD/turn)
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

    # ── stato chiamata (per EOC) ──
    loop = asyncio.get_running_loop()
    state = {"ended_reason": "completed", "start": loop.time(), "last_activity": loop.time()}

    def _touch(*_args) -> None:
        state["last_activity"] = loop.time()

    # Reset del timer di silenzio su attività (utente/agente). Lo stato "thinking"
    # (tool call) conta come attività → di fatto sospende il timeout durante i tool.
    session.on("user_state_changed", _touch)
    session.on("agent_state_changed", _touch)

    # Ri-arma il filler a ogni nuovo turno utente (final transcript) → max 1 frase d'attesa
    # per turno, come Boss. I tool successivi nello stesso turno li copre la musichetta.
    def _on_user_text(ev) -> None:
        _touch()
        if getattr(ev, "is_final", False):
            agent._filler_armed = True

    session.on("user_input_transcribed", _on_user_text)

    # Il lead riaggancia → chiusura con motivo dedicato.
    def _on_disconnect(participant) -> None:
        ident = getattr(participant, "identity", "")
        if ident.startswith("sip-") or str(getattr(participant, "kind", "")).upper().endswith("SIP"):
            state["ended_reason"] = "customer_hangup"
            asyncio.create_task(_hangup(ctx))

    ctx.room.on("participant_disconnected", _on_disconnect)

    # 4) End-of-call: POST al webhook EOC alla chiusura del job.
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
            # contesto utile al workflow di qualificazione
            "lead_name": md.get("lead_name"),
            "lead_phone": md.get("lead_phone"),
            "attempt_number": md.get("attempt_number"),
            "agent": AGENT_NAME,
            # esito AMD (None se AMD disabilitato/non disponibile): permette al workflow
            # di qualificazione di separare voicemail/IVR dai pickup umani reali.
            "amd_category": state.get("amd_category"),
        }
        logger.info("EOC POST (reason=%s, dur=%ss)", state["ended_reason"], duration)
        await _post_json(EOC_WEBHOOK_URL, payload)

    ctx.add_shutdown_callback(_send_eoc)

    # 5) Avvia la sessione
    await session.start(agent=agent, room=ctx.room)

    # Musichetta d'attesa: parte da sola quando l'agente "pensa" (tool in corso) e si ferma
    # da sola quando ricomincia a parlare. Canale audio separato → maschera la latenza SENZA
    # rischio di zittire l'agente. Complementare al filler vocale.
    try:
        background_audio = BackgroundAudioPlayer(thinking_sound=THINKING_SOUND)
        await background_audio.start(room=ctx.room, agent_session=session)
    except Exception as e:  # noqa: BLE001
        logger.warning("musichetta d'attesa non avviata: %s", e)

    # 6) Outbound first turn con AMD: attende il lead, classifica umano/segreteria/IVR,
    #    e SOLO se risponde una PERSONA (o esito incerto) apre la conversazione.
    #    Se AMD è disabilitato o non presente nella versione installata → flusso originale.
    if _AMD_AVAILABLE and AMD_ENABLED:
        # ivr_detection=False: non navighiamo alberi IVR (consumer outbound). Cosa fare su
        # machine-ivr (riagganciare o trattare come umano) è governato da AMD_IVR_HANGUP.
        # llm: AMD_LLM_MODEL se impostata, altrimenti default LiveKit Inference.
        # detection_options: solo le soglie effettivamente impostate via ENV.
        # suppress_compatibility_warning=True: silenzia il warning sui modelli di classifica.
        amd_kwargs: dict = {"ivr_detection": False, "suppress_compatibility_warning": True}
        if AMD_LLM_MODEL:
            amd_kwargs["llm"] = AMD_LLM_MODEL
        if AMD_DETECTION_OPTIONS:
            amd_kwargs["detection_options"] = AMD_DETECTION_OPTIONS
        async with AMD(session, **amd_kwargs) as detector:
            # Il SIP participant lo crea il dispatcher; qui attendiamo solo che si connetta.
            try:
                await asyncio.wait_for(ctx.wait_for_participant(), timeout=90)
            except asyncio.TimeoutError:
                logger.warning("Nessuna risposta dal lead entro il timeout")
                state["ended_reason"] = "no_answer"
                await _hangup(ctx)
                return

            # Classifica la chiamata (gira una volta, sul saluto iniziale).
            try:
                result = await detector.execute()
                category = getattr(result, "category", "uncertain")
            except Exception as e:  # noqa: BLE001
                logger.warning("AMD fallita (%s) → procedo come umano", e)
                category = "uncertain"
            state["amd_category"] = category
            logger.info("AMD category=%s", category)

            # human/uncertain → conversazione normale.
            # machine-ivr → riaggancia solo se AMD_IVR_HANGUP, altrimenti tratta come umano.
            # machine-vm / machine-unavailable → niente conversazione.
            hangup_categories = {"machine-vm", "machine-unavailable"}
            if AMD_IVR_HANGUP:
                hangup_categories.add("machine-ivr")

            if category in hangup_categories:
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
            # altrimenti (human/uncertain, o machine-ivr con AMD_IVR_HANGUP=false) → prosegui
    else:
        # AMD off o non disponibile: comportamento originale (attendi il partecipante).
        if AMD_ENABLED and not _AMD_AVAILABLE:
            logger.warning("AMD richiesto ma non disponibile in questa versione di "
                           "livekit-agents → flusso senza rilevamento segreteria")
        try:
            await asyncio.wait_for(ctx.wait_for_participant(), timeout=90)
        except asyncio.TimeoutError:
            logger.warning("Nessuna risposta dal lead entro il timeout")
            state["ended_reason"] = "no_answer"
            await _hangup(ctx)
            return

    # Watchdog silenzio (auto-hangup ~191s)
    async def _silence_watchdog() -> None:
        while True:
            await asyncio.sleep(5)
            if loop.time() - state["last_activity"] > SILENCE_TIMEOUT_S:
                logger.info("Silence timeout (%ss) → hangup", SILENCE_TIMEOUT_S)
                state["ended_reason"] = "silence_timeout"
                await _hangup(ctx)
                return

    watchdog_task = asyncio.create_task(_silence_watchdog())
    ctx.add_shutdown_callback(lambda: _cancel(watchdog_task))

    _touch()
    # L'agente parla per primo (STEP 1 del prompt). Niente firstMessage fisso.
    await session.generate_reply(
        instructions=(
            "È una chiamata in USCITA: inizi tu. Apri la conversazione seguendo STEP 1 "
            "del tuo system prompt (presentazione e contesto), in italiano, calda e naturale. "
            "Usa il nome della cliente se disponibile. Una o due frasi, poi attendi la risposta."
        )
    )


async def _cancel(task: asyncio.Task) -> None:
    task.cancel()


async def _hangup(ctx: JobContext) -> None:
    """Chiude la chiamata cancellando la room (fa scattare gli shutdown callback → EOC)."""
    try:
        await ctx.api.room.delete_room(api.DeleteRoomRequest(room=ctx.room.name))
    except Exception:  # noqa: BLE001
        logger.exception("delete_room fallita")


def _build_transcript(session: AgentSession) -> tuple[list[dict], str]:
    """Serializza la cronologia della conversazione per il payload EOC."""
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
