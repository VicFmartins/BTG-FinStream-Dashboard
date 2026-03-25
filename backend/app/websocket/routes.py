from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.services.dlq_store import DeadLetterQueueStore
from app.services.event_store import EventStore
from app.services.live_metrics import LiveMetricsService
from app.services.ops_metrics import OperationalMetricsService
from app.websocket.manager import ConnectionManager

router = APIRouter()


@router.websocket("/ws/transactions")
async def transaction_stream(websocket: WebSocket) -> None:
    manager: ConnectionManager = websocket.app.state.connection_manager
    event_store: EventStore = websocket.app.state.event_store
    live_metrics: LiveMetricsService = websocket.app.state.live_metrics
    ops_metrics: OperationalMetricsService = websocket.app.state.ops_metrics
    dlq_store: DeadLetterQueueStore = websocket.app.state.dlq_store

    await manager.connect(websocket)
    await manager.send_snapshot(
        websocket,
        event_store.latest_event(),
        live_metrics.snapshot(),
        ops_metrics.snapshot(),
        dlq_store.recent(limit=5),
    )

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await manager.disconnect(websocket)
