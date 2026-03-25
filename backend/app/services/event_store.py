from threading import Lock

from app.schemas.transaction_event import TransactionEvent


class EventStore:
    def __init__(self) -> None:
        self._lock = Lock()
        self._latest_event: TransactionEvent | None = None

    def record(self, event: TransactionEvent) -> None:
        with self._lock:
            self._latest_event = event

    def latest_event(self) -> TransactionEvent | None:
        with self._lock:
            return self._latest_event
