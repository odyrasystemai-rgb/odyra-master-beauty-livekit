# Eva (Master Beauty Agent) — Spec dei 13 tool per il port su LiveKit

Riferimento per scrivere le `@function_tool` nell'`agent.py` di Eva.
Sorgente: assistant Vapi `8e5422b9-b1d0-47cb-b80d-eedc06ecc53c`.

**Basi URL**
- Tool gestionale/booking → `https://primary-production-eb20.up.railway.app/webhook/...`
- Knowledge (RAG) → `https://rag-production-6ab5.up.railway.app/` (host DIVERSO)

**Regola trasversale:** ogni tool gestionale richiede `tenant_id`. In Vapi arriva da `{{tenant_id}}`. Su LiveKit **NON è un argomento del modello**: lo inietti dai metadata della room (il pacchetto `variableValues`). Idem `phone` del lead quando serve.

---

## A. Knowledge & utility

### 1. knowledge_query  (Vapi: `knowledge_query_generico`, tipo apiRequest "RAG")
- `POST https://rag-production-6ab5.up.railway.app/`
- body: `{ "id": <tenant_id>, "query": <stringa> }`
- ritorna: `{ "answer": "..." }`
- Nota: `id` = tenant_id. `query` = frase completa del cliente, mai una parola sola.

### 2. get_data_oggi  (tipo code, NESSUN webhook)
- È puro JS che calcola oggi/domani. Su LiveKit → semplice funzione Python con `datetime` (Europe/Rome). Nessuna chiamata HTTP.
- In pratica con il blocco `current_context` già nei metadata, questo tool diventa quasi superfluo.

---

## B. Ricerca disponibilità

### 3. cerca_disponibilita_settimana  (Vapi: `..._odyra_Studio`, tipo code)
- `POST .../webhook/core-cerca-disponibilita`
- args modello: `phone`, `tenant_id`, `duration_min`, `operator_slug`
- ⚠️ Il tool "code" PRE-ELABORA: calcola `date_from` (domani 00:00) e `date_to` (+7 giorni) in ISO con `+01:00`, poi POSTa:
  `{ tenant_id, duration_min, operator_slug, date_from, date_to, phone }`
- ritorno: `data.results[0].result`

### 4. controllo_disponibilita  (Vapi: `controllo_disponibilita_odyra_studio`, function)
- `POST .../webhook/core-disponibilita-slot-specifico`
- params: `prefareDateTime` (ISO), `duration_min`, `operator_slug`, `tenant_id`, `phone_number`, `email` (opz)
- È questo il tool per "giorno/ora preciso". (Nel prompt è chiamato impropriamente `cerca_disponibilita` — vedi note in fondo.)

### 5. cerca_disponibilita_multi  (Vapi: `..._odyra_studio`, tipo code)
- `POST .../webhook/core-cerca-disponibilita-multi`
- args modello: `phone`, `tenant_id`, `service_1_name`, `service_1_duration_min`, `service_1_operator_slug`, idem `service_2_*` (obbligatori), `service_3_*` (opz)
- ⚠️ code: costruisce `date_from`/`date_to` ISO e un array `services[]` `{name, duration_min, operator_slug, operator_name}`, poi POSTa `{ tenant_id, date_from, date_to, phone, services }`.

---

## C. Prenotazione

### 6. prenotazione_appuntamento  (Vapi: `prenotazione_appuntamento_odyra_studio`, function)
- `POST .../webhook/core-prenotazione-appuntamento`
- params: `prefareDateTime`, `name`, `phone`, `service`, `duration_min`, `operator_slug`, `tenant_id`, `email` (opz)

### 7. prenotazione_multi  (Vapi: `prenotazione_multi_odyra_studio`, tipo code)
- `POST .../webhook/core-prenotazione-multi`
- args modello: `name`, `phone`, `tenant_id`, e per ogni slot `slot_N_service`, `slot_N_start_iso`, `slot_N_end_iso`, `slot_N_operator_slug`, `slot_N_operator_name`, `slot_N_odoo_partner_id`, `slot_N_odoo_user_id` (N=1,2 obbligatori; 3 opz)
- ⚠️ code: assembla `slots[]` e POSTa `{ tenant_id, name, phone, slots }`.

---

## D. Spostamento

### 8. riprenotazione_appuntamento  (Vapi: `riprenotazione_appuntamento_odyra_studio`, function)
- `POST .../webhook/core-spostamento-appuntamento`
- params: `event_id ` ⚠️(la chiave ha uno SPAZIO finale, da preservare), `phone`, `service`, `duration_min`, `operator_slug`, `orario`, `tenant_id`, `new_datetime`, `email` (opz)

### 9. spostamento_multi  (Vapi: `spostamento_multi_odyra_Studio`, tipo code)
- `POST .../webhook/core-spostamento-multi`
- args modello: `name`, `phone`, `tenant_id`, `event_id_1`, `event_id_2` (`event_id_3` opz), e gli `slot_N_*` come prenotazione_multi
- ⚠️ code: assembla `event_ids[]` e `slots[]`, poi POSTa `{ tenant_id, name, phone, event_ids, slots }`.

---

## E. Cancellazione

### 10. cancellazione_appuntamento  (Vapi: `cancellazione_appuntamento_odyra_studio`, function)
- `POST .../webhook/core-cancellazione-appuntamento`
- params: `event_id ` ⚠️(SPAZIO finale), `phone`, `tenant_id`, `email` (opz)

### 11. cancellazione_multi  (Vapi: `cancellazione_multi_odyra_studio`, tipo code)
- `POST .../webhook/core-cancellazione-multi`
- args modello: `tenant_id`, `phone`, `event_id_1`, `event_id_2` (`event_id_3` opz)
- ⚠️ code: assembla `event_ids[]`, POSTa `{ tenant_id, event_ids }`.

---

## F. Verifica & handoff

### 12. customer_verification  (Vapi: `customer_verification_odyra_studio`, apiRequest)
- `POST .../webhook/core-customer-verification`
- body: `{ phone, tenant_id }`

### 13. trasferisci_operatore  (Vapi: `trasferisci_operatore_odyra_studio`, function)
- `POST .../webhook/core-handoff-resolver`
- params: `motivo`, `tenant_id`
- Nota: su LiveKit il trasferimento di chiamata vero (SIP transfer al `handoff_number`) va gestito lato agente; questo webhook è il "resolver" che logga/risolve il numero.

---

## Note per il port su LiveKit (importanti)

1. **Tool "code" ≠ passthrough.** I tool di tipo `code` (settimana, multi, get_data_oggi) NON girano su un webhook: contengono JS che trasforma gli argomenti (calcolo date ISO `+01:00`, costruzione array `services`/`slots`) e POI fanno `fetch`. In `agent.py` quella logica di trasformazione va replicata in Python dentro la `@function_tool`, prima del POST allo stesso webhook.

2. **`tenant_id` e `phone` dai metadata.** Non farli decidere al modello: iniettali dal pacchetto `variableValues` della room. Meno errori, più pulito.

3. **Chiavi con spazio finale.** `event_id ` (spostamento e cancellazione singola) ha uno spazio finale nella chiave: va preservato esatto, altrimenti il webhook n8n non lo legge.

4. **Naming.** In Vapi i nomi funzione hanno suffissi (`_odyra_Studio`/`_odyra_studio`), ma il prompt li chiama generici (`prenotazione_appuntamento`, ecc.). Su LiveKit conviene usare i **nomi generici del prompt**: così prompt e tool combaciano meglio che su Vapi.

5. **Incongruenza prompt↔tool da sistemare.** Il prompt cita `cerca_disponibilita` (giorno preciso, con `date_from/date_to`) ma quel tool NON è agganciato: il tool reale per il giorno preciso è `controllo_disponibilita` (slot specifico, `prefareDateTime`). In fase di rebuild: o si allinea il prompt, o si crea davvero un `cerca_disponibilita` a giorno singolo.

6. **knowledge_query host diverso.** Punta al servizio RAG (`rag-production-6ab5`), non al base n8n. Il campo si chiama `id` (= tenant_id), non `tenant_id`.
