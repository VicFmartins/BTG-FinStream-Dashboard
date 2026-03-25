import asyncio
import contextlib
import logging

from fastapi import WebSocket

from app.schemas.operations import DeadLetterEvent, OperationalMetricsSnapshot
from app.schemas.transaction_event import LiveMetricsSnapshot, TransactionEvent

logger = logging.getLogger(__name__)


class ConnectionManager:
    def __init__(self) -> None:
        self._connections: set[WebSocket] = set()
        self._lock = asyncio.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self._connections.add(websocket)
            total_connections = len(self._connections)

        logger.info("WebSocket client connected. active_connections=%s", total_connections)

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            self._connections.discard(websocket)
            total_connections = len(self._connections)

        logger.info("WebSocket client disconnected. active_connections=%s", total_connections)

    def publish_transaction_event(
        self,
        event: TransactionEvent,
        metrics: LiveMetricsSnapshot,
        ops: OperationalMetricsSnapshot,
    ) -> None:
        if self._loop is None or self._loop.is_closed():
            return

        message = {
            "type": "transaction_event",
            "event": event.model_dump(),
            "metrics": metrics.model_dump(),
            "ops": ops.model_dump(),
        }
        asyncio.run_coroutine_threadsafe(self._broadcast(message), self._loop)

    def publish_operational_update(
        self,
        ops: OperationalMetricsSnapshot,
        dlq: list[DeadLetterEvent] | None = None,
    ) -> None:
        if self._loop is None or self._loop.is_closed():
            return

        message = {
            "type": "ops_update",
            "ops": ops.model_dump(),
        }
        if dlq is not None:
            message["dlq"] = [item.model_dump() for item in dlq]

        asyncio.run_coroutine_threadsafe(self._broadcast(message), self._loop)

    async def send_snapshot(
        self,
        websocket: WebSocket,
        event: TransactionEvent | None,
        metrics: LiveMetricsSnapshot,
        ops: OperationalMetricsSnapshot,
        dlq: list[DeadLetterEvent],
    ) -> None:
        await websocket.send_json(
            {
                "type": "snapshot",
                "event": event.model_dump() if event else None,
                "metrics": metrics.model_dump(),
                "ops": ops.model_dump(),
                "dlq": [item.model_dump() for item in dlq],
            }
        )

    async def _broadcast(self, message: dict[str, object]) -> None:
        async with self._lock:
            connections = list(self._connections)

        if not connections:
            return

        stale_connections: list[WebSocket] = []

        for connection in connections:
            try:
                await connection.send_json(message)
            except Exception:
                stale_connections.append(connection)

        for connection in stale_connections:
            with contextlib.suppress(Exception):
                await connection.close()
            await self.disconnect(connection)
