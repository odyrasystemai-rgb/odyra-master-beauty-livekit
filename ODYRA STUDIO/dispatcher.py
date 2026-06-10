"""
Dispatcher HTTP per Eva (outbound) — endpoint chiamato da n8n.

Co-locato col worker. Espone:

    POST /outbound/start
    Header: Authorization: Bearer <LIVEKIT_DISPATCH_SECRET>
    body: { agent_name, tenant_id, phone, from_number, metadata: {…} }
    → 200 { "ok": true, "call_id": "<room>", "room": "<room>" }

Cosa fa (con il server SDK livekit.api):
  1. Verifica il Bearer.
  2. Crea una room univoca (eva-out-<lead_id>-<ts>).
  3. Crea un agent dispatch esplicito per "eva-outbound" con metadata = json.dumps(metadata).
  4. Crea un SIP participant in uscita (trunk Eva, numero = phone) nella room.
  5. Risponde { ok, call_id, room }.
"""

from __future__ import annotations

import json
import logging
import os
import time

from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Request

from livekit import api

load_dotenv()

logger = logging.getLogger("eva-dispatcher")
logging.basicConfig(level=logging.INFO)

AGENT_NAME = "eva-outbound"
DISPATCH_SECRET = os.environ.get("LIVEKIT_DISPATCH_SECRET", "")
SIP_OUTBOUND_TRUNK_ID = os.getenv("SIP_OUTBOUND_TRUNK_ID", "ST_Yuv6LbXJTLno")
SIP_FROM_NUMBER = os.getenv("SIP_FROM_NUMBER", "+390230329429")

app = FastAPI(title="Eva Outbound Dispatcher")


def _slug(value: str) -> str:
    return "".join(c if c.isalnum() else "-" for c in str(value)).strip("-") or "lead"


@app.get("/health")
async def health() -> dict:
    return {"ok": True, "agent": AGENT_NAME}


@app.post("/outbound/start")
async def outbound_start(request: Request, authorization: str = Header(default="")) -> dict:
    # 1) Verifica Bearer
    if not DISPATCH_SECRET or authorization != f"Bearer {DISPATCH_SECRET}":
        raise HTTPException(status_code=401, detail="unauthorized")

    body = await request.json()
    agent_name = body.get("agent_name") or AGENT_NAME
    phone = body.get("phone")
    if not phone:
        raise HTTPException(status_code=400, detail="missing 'phone'")

    metadata = body.get("metadata") or {}
    # tenant_id può arrivare top-level o nei metadata: garantiamo che sia nei metadata.
    if body.get("tenant_id") and "tenant_id" not in metadata:
        metadata["tenant_id"] = body["tenant_id"]

    from_number = body.get("from_number") or SIP_FROM_NUMBER
    lead_id = metadata.get("lead_id") or body.get("tenant_id") or "lead"

    # 2) Room univoca
    ts = int(time.time())
    room = f"eva-out-{_slug(lead_id)}-{ts}"

    lkapi = api.LiveKitAPI()  # legge LIVEKIT_URL / LIVEKIT_API_KEY / LIVEKIT_API_SECRET dall'env
    try:
        await lkapi.room.create_room(api.CreateRoomRequest(name=room))

        # 3) Agent dispatch esplicito per eva-outbound, con i metadata della chiamata
        await lkapi.agent_dispatch.create_dispatch(
            api.CreateAgentDispatchRequest(
                agent_name=agent_name,
                room=room,
                metadata=json.dumps(metadata),
            )
        )

        # 4) SIP participant in uscita: il trunk Eva chiama il numero del lead.
        #    NB: il caller ID (+390230329429) è configurato sul trunk outbound Eva.
        #    Un override per-chiamata (from_number) richiede config a livello di trunk;
        #    lo passiamo nei metadata per tracciabilità.
        if from_number:
            metadata.setdefault("from_number", from_number)
        await lkapi.sip.create_sip_participant(
            api.CreateSIPParticipantRequest(
                sip_trunk_id=SIP_OUTBOUND_TRUNK_ID,
                sip_call_to=phone,
                room_name=room,
                participant_identity=f"sip-{_slug(lead_id)}",
                participant_name=metadata.get("lead_name") or "lead",
                wait_until_answered=False,
            )
        )
    finally:
        await lkapi.aclose()

    logger.info("Dispatch ok: room=%s phone=%s agent=%s", room, phone, agent_name)
    # 5) Risposta
    return {"ok": True, "call_id": room, "room": room}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "dispatcher:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8080")),
    )
