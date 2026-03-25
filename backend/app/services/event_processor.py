import logging

from app.schemas.operations import OperationalMetricsSnapshot
from app.schemas.transaction_event import TransactionEvent
from app.services.event_repository import EventRepository
from app.services.event_store import EventStore
from app.services.live_metrics import LiveMetricsService
from app.services.ops_metrics import OperationalMetricsService
from app.websocket.manager import ConnectionManager

logger = logging.getLogger(__name__)


class TransactionEventProcessor:
    def __init__(
        self,
        event_store: EventStore,
        live_metrics: LiveMetricsService,
        ops_metrics: OperationalMetricsService,
        event_repository: EventRepository | None = None,
        connection_manager: ConnectionManager | None = None,
    ) -> None:
        self.event_store = event_store
        self.live_metrics = live_metrics
        self.ops_metrics = ops_metrics
        self.event_repository = event_repository
        self.connection_manager = connection_manager

    def process(self, event: TransactionEvent) -> None:
        self.ops_metrics.record_valid_event()
        inserted, ops = self._persist_event(event)
        if inserted is False:
            if self.connection_manager is not None and ops is not None:
                self.connection_manager.publish_operational_update(ops)
            return

        self.event_store.record(event)
        metrics = self.live_metrics.record(event)
        ops = self.ops_metrics.record_persisted_event(event.timestamp)
        self._log_event(event, metrics.total_processed_events, metrics.total_transaction_volume)

        if self.connection_manager is not None:
            self.connection_manager.publish_transaction_event(event, metrics, ops)

    def _log_event(
        self,
        event: TransactionEvent,
        total_processed_events: int,
        total_transaction_volume: float,
    ) -> None:
        logger.info(
            "Received transaction event event_id=%s client_id=%s asset=%s event_type=%s unit_price=%.4f quantity=%s notional_amount=%.2f timestamp=%s total_processed_events=%s total_transaction_volume=%.2f",
            event.event_id,
            event.client_id,
            event.asset,
            event.event_type,
            event.unit_price,
            event.quantity,
            event.notional_amount,
            event.timestamp,
            total_processed_events,
            total_transaction_volume,
        )

    def _persist_event(self, event: TransactionEvent) -> tuple[bool | None, OperationalMetricsSnapshot | None]:
        if self.event_repository is None:
            return True, self.ops_metrics.snapshot()

        try:
            inserted = self.event_repository.save(event)
        except Exception:
            logger.exception(
                "Failed to persist transaction event event_id=%s without stopping the live flow.",
                event.event_id,
            )
            return None, self.ops_metrics.snapshot()

        if inserted:
            logger.info("Persisted transaction event event_id=%s", event.event_id)
        else:
            logger.info("Skipping duplicate transaction event event_id=%s", event.event_id)
            ops = self.ops_metrics.record_duplicate_event()
            return False, ops

        return True, self.ops_metrics.snapshot()
