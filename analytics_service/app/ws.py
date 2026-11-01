"""Pushes a snapshot to every open dashboard 4 times a second."""

import asyncio
import json
from fastapi import WebSocket
from app import config
from app.state import analytics


class DashboardHub:
    def __init__(self) -> None:
        self.clients: set[WebSocket] = set()
        self._task: asyncio.Task | None = None

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self.clients.add(ws)
        await ws.send_text(json.dumps(analytics.snapshot()))

    def disconnect(self, ws: WebSocket) -> None:
        self.clients.discard(ws)

    def start(self) -> None:
        self._task = asyncio.create_task(self._broadcast_loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def _broadcast_loop(self) -> None:
        """One snapshot per tick -- never one message per event, that would melt
        the browser at 10,000 messages a second."""
        while True:
            await asyncio.sleep(config.BROADCAST_INTERVAL_SEC)
            if not self.clients:
                continue
            payload = json.dumps(analytics.snapshot())
            for ws in list(self.clients):
                try:
                    await ws.send_text(payload)
                except Exception:
                    self.clients.discard(ws)


hub = DashboardHub()
