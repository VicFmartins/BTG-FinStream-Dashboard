from collections import deque
from datetime import UTC, datetime
from threading import Lock

from app.schemas.operations import DeadLetterEvent


class DeadLetterQueueStore:
    def __init__(self, max_items: int = 50) -> None:
        self._lock = Lock()
        self._items: deque[DeadLetterEvent] = deque(maxlen=max_items)

    def record(self, payload: dict[str, object], error: str) -> DeadLetterEvent:
        item = DeadLetterEvent(
            received_at=datetime.now(UTC).isoformat(),
            error=error,
            payload=payload,
        )

        with self._lock:
            self._items.appendleft(item)

        return item

    def recent(self, limit: int = 20) -> list[DeadLetterEvent]:
        with self._lock:
            return list(self._items)[:limit]

    def size(self) -> int:
        with self._lock:
            return len(self._items)
