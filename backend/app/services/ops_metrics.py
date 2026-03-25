from threading import Lock

from app.schemas.operations import OperationalMetricsSnapshot


class OperationalMetricsService:
    def __init__(self) -> None:
        self._lock = Lock()
        self._total_valid_events = 0
        self._total_invalid_events = 0
        self._total_persisted_events = 0
        self._duplicate_events_skipped = 0
        self._last_successful_event_timestamp: str | None = None
        self._last_invalid_event_timestamp: str | None = None

    def record_valid_event(self) -> OperationalMetricsSnapshot:
        with self._lock:
            self._total_valid_events += 1
            return self._snapshot_unlocked()

    def record_invalid_event(self, timestamp: str) -> OperationalMetricsSnapshot:
        with self._lock:
            self._total_invalid_events += 1
            self._last_invalid_event_timestamp = timestamp
            return self._snapshot_unlocked()

    def record_persisted_event(self, timestamp: str) -> OperationalMetricsSnapshot:
        with self._lock:
            self._total_persisted_events += 1
            self._last_successful_event_timestamp = timestamp
            return self._snapshot_unlocked()

    def record_duplicate_event(self) -> OperationalMetricsSnapshot:
        with self._lock:
            self._duplicate_events_skipped += 1
            return self._snapshot_unlocked()

    def snapshot(self) -> OperationalMetricsSnapshot:
        with self._lock:
            return self._snapshot_unlocked()

    def _snapshot_unlocked(self) -> OperationalMetricsSnapshot:
        return OperationalMetricsSnapshot(
            total_valid_events=self._total_valid_events,
            total_invalid_events=self._total_invalid_events,
            total_persisted_events=self._total_persisted_events,
            duplicate_events_skipped=self._duplicate_events_skipped,
            last_successful_event_timestamp=self._last_successful_event_timestamp,
            last_invalid_event_timestamp=self._last_invalid_event_timestamp,
        )
