from pydantic import BaseModel


class OperationalMetricsSnapshot(BaseModel):
    total_valid_events: int
    total_invalid_events: int
    total_persisted_events: int
    duplicate_events_skipped: int
    last_successful_event_timestamp: str | None
    last_invalid_event_timestamp: str | None


class DeadLetterEvent(BaseModel):
    received_at: str
    error: str
    payload: dict[str, object]


class OperationalHealthResponse(BaseModel):
    status: str
    consumer_enabled: bool
    kafka_brokers: str
    topic: str
    latest_successful_event_timestamp: str | None
    latest_invalid_event_timestamp: str | None
    total_invalid_events: int
    duplicate_events_skipped: int
    dlq_size: int
