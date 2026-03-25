from fastapi import APIRouter, Request

from app.core.config import get_settings
from app.schemas.operations import DeadLetterEvent, OperationalHealthResponse, OperationalMetricsSnapshot
from app.services.dlq_store import DeadLetterQueueStore
from app.services.ops_metrics import OperationalMetricsService

router = APIRouter(prefix="/ops", tags=["ops"])


@router.get("/health", response_model=OperationalHealthResponse)
def ops_health(request: Request) -> OperationalHealthResponse:
    settings = get_settings()
    ops_metrics: OperationalMetricsService = request.app.state.ops_metrics
    dlq_store: DeadLetterQueueStore = request.app.state.dlq_store
    snapshot = ops_metrics.snapshot()

    status = "healthy" if settings.enable_event_consumer else "idle"
    if snapshot.total_invalid_events > 0:
        status = "degraded"

    return OperationalHealthResponse(
        status=status,
        consumer_enabled=settings.enable_event_consumer,
        kafka_brokers=settings.kafka_brokers,
        topic=settings.event_topic,
        latest_successful_event_timestamp=snapshot.last_successful_event_timestamp,
        latest_invalid_event_timestamp=snapshot.last_invalid_event_timestamp,
        total_invalid_events=snapshot.total_invalid_events,
        duplicate_events_skipped=snapshot.duplicate_events_skipped,
        dlq_size=dlq_store.size(),
    )


@router.get("/metrics", response_model=OperationalMetricsSnapshot)
def ops_metrics(request: Request) -> OperationalMetricsSnapshot:
    ops_metrics_service: OperationalMetricsService = request.app.state.ops_metrics
    return ops_metrics_service.snapshot()


@router.get("/dlq", response_model=list[DeadLetterEvent])
def ops_dlq(request: Request) -> list[DeadLetterEvent]:
    dlq_store: DeadLetterQueueStore = request.app.state.dlq_store
    return dlq_store.recent(limit=20)
