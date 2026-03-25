from threading import Lock

from app.schemas.transaction_event import LiveMetricsSnapshot, TransactionEvent

DEFAULT_EVENT_COUNTS = {
    "BUY": 0,
    "SELL": 0,
    "DEPOSIT": 0,
    "WITHDRAWAL": 0,
}


class LiveMetricsService:
    def __init__(self) -> None:
        self._lock = Lock()
        self._total_processed_events = 0
        self._total_transaction_volume = 0.0
        self._events_by_type = DEFAULT_EVENT_COUNTS.copy()
        self._latest_client_id: str | None = None
        self._latest_asset: str | None = None
        self._latest_event_timestamp: str | None = None

    def record(self, event: TransactionEvent) -> LiveMetricsSnapshot:
        with self._lock:
            self._total_processed_events += 1
            self._total_transaction_volume += event.amount
            self._events_by_type[event.event_type] += 1
            self._latest_client_id = event.client_id
            self._latest_asset = event.asset
            self._latest_event_timestamp = event.timestamp
            return self._snapshot_unlocked()

    def snapshot(self) -> LiveMetricsSnapshot:
        with self._lock:
            return self._snapshot_unlocked()

    def _snapshot_unlocked(self) -> LiveMetricsSnapshot:
        return LiveMetricsSnapshot(
            total_processed_events=self._total_processed_events,
            total_transaction_volume=round(self._total_transaction_volume, 2),
            events_by_type=self._events_by_type.copy(),
            latest_client_id=self._latest_client_id,
            latest_asset=self._latest_asset,
            latest_event_timestamp=self._latest_event_timestamp,
        )
