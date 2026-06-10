# Eva — Agente vocale LiveKit (outbound, beauty/estetica)

Worker LiveKit OUTBOUND multi-tenant che replica il "Master Beauty Agent" (ex Vapi).
Chiama lead, qualifica, fissa appuntamenti. I dati del tenant + del lead arrivano a
**runtime nei metadata della room** (il pacchetto `variableValues` del nodo n8n
"Chiamata Voce v2"), non da lookup su DB.

`agent_name = "eva-outbound"`

## File

| File | Cosa fa |
|------|---------|
| `agent.py` | Worker LiveKit: sessione STT/LLM/TTS/VAD/turn, system prompt da `EVA_system_prompt.txt` con sostituzione `{{placeholder}}` dai metadata, 14 `@function_tool`, silence-timeout ~191s, end-of-call webhook. |
| `dispatcher.py` | Servizio HTTP (FastAPI) chiamato da n8n: `POST /outbound/start` → crea room, agent dispatch e SIP participant in uscita. |
| `EVA_system_prompt.txt` | Template del system prompt (con `{{placeholder}}`). |
| `EVA_tools_spec.md` / `EVA_build_brief.md` | Spec di riferimento. |
| `requirements.txt`, `Dockerfile`, `.env.example` | Dipendenze e deploy. |

## Configurazione

Copia `.env.example` → `.env` e compila le chiavi (LiveKit, OpenAI, Cartesia, Deepgram,
`LIVEKIT_DISPATCH_SECRET`). Trunk e caller ID Eva sono già impostati di default.

## Stack

- **LLM:** OpenAI `gpt-4.1-mini`
- **TTS:** Cartesia, voice `36d94908-c5b9-4014-b521-e69aee5bead0` (model default `sonic-2`,
  imposta `CARTESIA_MODEL=sonic-3.5` se il plugin lo accetta). Controlli espressivi
  (`speed=fastest`, emotion) applicati se supportati, con fallback automatico.
- **STT:** Deepgram `nova-3`, `language="it"`, `numerals=True`, keyterm list inclusa.
- **VAD/turn:** Silero VAD + turn-detector multilingue.
- **SIP outbound:** trunk `ST_Yuv6LbXJTLno`, caller ID `+390230329429`.

## Tool implementati (14)

`knowledge_query` (host RAG, campo `id`=tenant_id), `get_data_oggi`,
`cerca_disponibilita` (giorno singolo → range 00:00–23:59 su `core-cerca-disponibilita`),
`controllo_disponibilita` (ora precisa, `prefareDateTime`),
`cerca_disponibilita_settimana`, `cerca_disponibilita_multi`,
`prenotazione_appuntamento`, `prenotazione_multi`,
`riprenotazione_appuntamento`, `spostamento_multi`,
`cancellazione_appuntamento`, `cancellazione_multi`,
`customer_verification`, `trasferisci_operatore` (resolver + transfer SIP).

`tenant_id` e `phone` sono iniettati dai metadata, non sono argomenti del modello.
Le chiavi `event_id ` (spostamento/cancellazione singola) preservano lo **spazio finale**.

## Esecuzione locale

```bash
pip install -r requirements.txt

# 1) Worker (modalità dev, connesso a odyra-poc)
python agent.py dev

# 2) Dispatcher (altro terminale)
uvicorn dispatcher:app --host 0.0.0.0 --port 8080
```

### Test end-to-end (senza toccare la produzione n8n)

Chiama il dispatcher con un `metadata` di test (tenant Eva reale) e il **tuo** numero:

```bash
curl -X POST http://localhost:8080/outbound/start \
  -H "Authorization: Bearer $LIVEKIT_DISPATCH_SECRET" \
  -H "Content-Type: application/json" \
  -d '{
    "agent_name": "eva-outbound",
    "phone": "+39XXXXXXXXXX",
    "metadata": {
      "tenant_id": "...",
      "lead_id": "test-1",
      "lead_name": "Maria",
      "lead_phone": "+39XXXXXXXXXX",
      "business_name": "...",
      "current_context": "Oggi è ...",
      "has_stripe": "false"
    }
  }'
```

Ti deve squillare il telefono, l'agente apre la conversazione e i tool rispondono.
Solo quando funziona end-to-end, su n8n imposta `PROVIDER_OVERRIDE='livekit'` e `LIVEKIT_DISPATCH_URL`.

## Deploy (Railway)

Due servizi dalla stessa immagine Docker:

- **Worker:** start command `python agent.py start`
- **Dispatcher:** start command `uvicorn dispatcher:app --host 0.0.0.0 --port $PORT`

## Note aperte (non bloccanti)

- **EOC dual-mode:** il payload inviato a `vapi-end-of-call` è "LiveKit-shaped"
  (`lead_id`, `tenant_id`, `duration_seconds`, `ended_reason`, `costEur`, `transcript`,
  `messages`). Da verificare che l'handler n8n lo accetti (come per Boss).
- **Transfer SIP:** `trasferisci_operatore` chiama il resolver e poi tenta un transfer SIP
  reale verso `handoff_number`. Se l'API di transfer non è abilitata nel progetto, il tool
  ritorna `transfer: failed/skipped` e si chiude con garbo (TODO segnato nel codice).
- **`costEur`:** stima `durata × EOC_COST_PER_MINUTE_EUR` (default 0); il costo "vero" lo calcola n8n.
