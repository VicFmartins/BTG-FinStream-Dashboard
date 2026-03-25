from typing import Literal

from fastapi import APIRouter, Query, Request

from app.core.config import get_settings
from app.schemas.transaction_event import EventStreamStatus, HistoricalTransactionEvent
from app.services.event_repository import EventRepository
from app.services.event_store import EventStore
from app.services.live_metrics import LiveMetricsService

router = APIRouter(prefix="/events", tags=["events"])


@router.get("/latest", response_model=EventStreamStatus)
def latest_event(request: Request) -> EventStreamStatus:
    settings = get_settings()
    event_store: EventStore = request.app.state.event_store
    live_metrics: LiveMetricsService = request.app.state.live_metrics

    return EventStreamStatus(
        consumer_enabled=settings.enable_event_consumer,
        kafka_brokers=settings.kafka_brokers,
        topic=settings.event_topic,
        latest_event=event_store.latest_event(),
        metrics=live_metrics.snapshot(),
    )


@router.get("/history", response_model=list[HistoricalTransactionEvent])
def event_history(
    request: Request,
    limit: int = Query(default=50, ge=1, le=200),
    client_id: str | None = None,
    asset: str | None = None,
    event_type: Literal["BUY", "SELL", "DEPOSIT", "WITHDRAWAL"] | None = None,
) -> list[HistoricalTransactionEvent]:
    event_repository: EventRepository = request.app.state.event_repository

    return event_repository.history(
        limit=limit,
        client_id=client_id,
        asset=asset,
        event_type=event_type,
    )
