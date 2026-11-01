"""Serves the dashboard page and its live WebSocket feed."""

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from app import config
from app.ws import hub

router = APIRouter(tags=["dashboard"])


@router.get("/", include_in_schema=False)
async def dashboard():
    return FileResponse(config.STATIC_DIR / "dashboard.html")


@router.websocket("/ws")
async def live_feed(ws: WebSocket):
    await hub.connect(ws)
    try:
        while True:
            await ws.receive_text()   # keeps the socket open
    except WebSocketDisconnect:
        pass
    finally:
        hub.disconnect(ws)
