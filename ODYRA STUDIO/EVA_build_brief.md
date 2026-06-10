# Brief per Claude Code — Agente LiveKit "Eva" (outbound, beauty/estetica)

> Da incollare in Claude Code insieme ai file `EVA_system_prompt.txt` e `EVA_tools_spec.md`.

## Obiettivo
Creare un **nuovo** agente vocale LiveKit OUTBOUND che replica il "Master Beauty Agent" oggi su Vapi. Chiama lead, qualifica, e fissa appuntamenti. Funziona in multi-tenant: i dati del tenant + del lead arrivano a runtime nei **metadata** della room (NON da lookup su DB per numero chiamato).

## Vincoli (NON negoziabili)
- **NON toccare l'agente Boss.** Niente modifiche a `agent.py` di Boss né al suo `SYSTEM_PROMPT_TEMPLATE`. Crea una cartella/progetto separato (es. `eva/`).
- Worker self-hosted (Railway) che si connette a **LiveKit Cloud** per media/SIP/observability.
- `agent_name = "eva-outbound"` (deve combaciare col nodo n8n già predisposto).
- Riusa come **scheletro** la struttura del worker Boss esistente (setup sessione, plugin, pattern end-of-call), adattando solo le differenze qui sotto.

## Config esatta
- **LiveKit:** URL `wss://odyra-poc-pei6rz4l.livekit.cloud`, Project `p_9xb774boesz`.
- **LLM:** OpenAI `gpt-4.1-mini`.
- **TTS:** Cartesia, voice `36d94908-c5b9-4014-b521-e69aee5bead0`, model `sonic-3.5` (se il plugin non accetta la stringa `3.5`, usa l'ultima `sonic` disponibile). Controlli espressivi originali: speed `fastest`, emotion `[positivity:highest, surprise:highest, curiosity:highest, anger:low, sadness:low]` (applicali se il plugin li supporta).
- **STT:** Deepgram `nova-3` (nova-3-general), `language="it"`, `numerals=True`, con keyterm list (in fondo a questo brief).
- **VAD/turn:** Silero VAD + turn-detector multilingue (come Boss).
- **Trunk SIP outbound (Eva):** `ST_Yuv6LbXJTLno` — caller ID `+390230329429`. (NON usare quello di Boss `ST_nJ8JnanQKVEv`.)
- **End-of-call webhook:** `https://primary-production-eb20.up.railway.app/webhook/vapi-end-of-call` (lo stesso che usava l'agente Beauty su Vapi — vedi nota EOC).

## Architettura: 2 componenti
### 1) `agent.py` — worker LiveKit (entrypoint)
1. All'avvio della sessione legge `ctx.job.metadata` → JSON con il pacchetto `variableValues` (vedi "Contratto metadata"). Fai `json.loads`.
2. Costruisci il system prompt: prendi `EVA_system_prompt.txt` come `SYSTEM_PROMPT_TEMPLATE` e **sostituisci ogni `{{chiave}}`** con `metadata["chiave"]` (placeholder mancante → stringa vuota). Stessa idea del riempimento di Boss, ma sorgente = metadata, non Supabase.
3. Avvia la sessione (STT/LLM/TTS/VAD/turn come sopra).
4. **Outbound first turn:** è una chiamata in uscita, l'agente parla per primo. Dopo che il SIP participant (il lead) è connesso, genera l'apertura seguendo STEP 1 del prompt (`generate_reply` con istruzione iniziale). Niente `firstMessage` fisso.
5. **Tool:** implementa le 13 `@function_tool` come da `EVA_tools_spec.md`. Punti chiave:
   - `tenant_id` e `phone` NON sono argomenti del modello: prendili dal metadata.
   - I tool "code" (settimana, multi, get_data_oggi) richiedono la **logica di trasformazione in Python** (calcolo date ISO `+01:00`, costruzione `services`/`slots`) prima del POST.
   - Preserva le chiavi esatte, inclusa `event_id ` con lo **spazio finale**.
   - `knowledge_query` → host RAG diverso, campo `id` (= tenant_id).
   - Usa i nomi generici (es. `prenotazione_appuntamento`) per allineare prompt e tool.
6. **Silence timeout:** auto-hangup a ~191s di silenzio (valore originale Beauty), con reset sul parlato e sospensione durante i tool call. (Stesso meccanismo che stiamo già usando su Boss.)
7. **End-of-call:** alla chiusura sessione, POST al webhook EOC con almeno: `lead_id`, `tenant_id`, `duration_seconds`, `ended_reason`, `costEur`, e la **trascrizione** (serve al workflow di qualificazione che genera hotness/summary). Replica il pattern EOC di Boss.

### 2) Dispatcher HTTP — endpoint chiamato da n8n
Un piccolo servizio (FastAPI/aiohttp) co-locato col worker, che espone:
```
POST /outbound/start
Header: Authorization: Bearer <LIVEKIT_DISPATCH_SECRET>
body: { agent_name, tenant_id, phone, from_number, metadata: {…} }
→ 200 { "ok": true, "call_id": "<room>", "room": "<room>" }
```
Cosa fa, con il server SDK `livekit.api`:
1. Verifica il Bearer.
2. Crea una room univoca (es. `eva-out-<lead_id>-<ts>`).
3. Crea un **agent dispatch** esplicito per `eva-outbound` con `metadata = json.dumps(body["metadata"])`.
4. Crea un **SIP participant** in uscita: trunk `ST_Yuv6LbXJTLno`, numero = `body["phone"]`, nella room (caller ID `+390230329429`, o `from_number` se passato).
5. Risponde `{ ok, call_id, room }`.

## Contratto metadata (il JSON che arriva nei metadata)
Esattamente l'oggetto `variableValues` costruito dal nodo n8n "Chiamata Voce v2". Chiavi: `tenant_id, location_id, business_name, business_type, city, staff_label, staff_label_upper, operators_list, business_hours, lead_id, lead_name, lead_phone, lead_email, lead_service, location_info, info, attempt_number, handoff_number, has_stripe, current_context` + tutte le chiavi `slot_*` (da `tenants.prompt_overrides`). Sono esattamente i `{{placeholder}}` del prompt.

## Variabili d'ambiente (.env)
`LIVEKIT_URL`, `LIVEKIT_API_KEY`, `LIVEKIT_API_SECRET`, `OPENAI_API_KEY`, `CARTESIA_API_KEY`, `DEEPGRAM_API_KEY`, `LIVEKIT_DISPATCH_SECRET`, `SIP_OUTBOUND_TRUNK_ID=ST_Yuv6LbXJTLno`, `SIP_FROM_NUMBER=+390230329429`, `EOC_WEBHOOK_URL=https://primary-production-eb20.up.railway.app/webhook/vapi-end-of-call`.

## Come testiamo (senza toccare la produzione n8n)
1. Avvia il worker in locale (`python agent.py dev`) connesso a `odyra-poc`.
2. Avvia il dispatcher (locale o Railway).
3. Chiama il dispatcher con `curl`, passando un `metadata` di test (tenant Eva reale) e il **tuo** numero come `phone`.
4. Verifica: ti squilla il telefono, l'agente apre la conversazione, i tool rispondono.
5. Solo quando funziona end-to-end, su n8n metti `PROVIDER_OVERRIDE='livekit'` e `LIVEKIT_DISPATCH_URL`.

## Da confermare con Riccardo (non bloccante per scrivere il codice)
- Worker Eva self-hosted su Railway (sì, come da architettura multi-software-house).
- Il webhook EOC `vapi-end-of-call` accetta payload "LiveKit-shaped"? (Su Boss avevamo un handler dual-mode Vapi/LiveKit — qui va verificato/replicato.)

---

### Keyterm Deepgram (lista originale)
chiocciola, punto, virgola, trattino, trattino basso, underscore, gmail, yahoo, hotmail, outlook, libero, alice, tim, virgilio, icloud, protonmail, email, posta elettronica, indirizzo mail, prefisso, cellulare, numero di telefono, zero, uno, due, tre, quattro, cinque, sei, sette, otto, nove
