import json
import logging
from threading import Event, Thread

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pydantic import ValidationError

from app.core.config import Settings
from app.services.dlq_store import DeadLetterQueueStore
from app.schemas.transaction_event import TransactionEvent
from app.services.event_processor import TransactionEventProcessor
from app.services.event_repository import EventRepository
from app.services.event_store import EventStore
from app.services.live_metrics import LiveMetricsService
from app.services.ops_metrics import OperationalMetricsService
from app.websocket.manager import ConnectionManager

logger = logging.getLogger(__name__)


class TransactionEventConsumer:
    def __init__(
        self,
        settings: Settings,
        event_store: EventStore,
        live_metrics: LiveMetricsService,
        ops_metrics: OperationalMetricsService,
        dlq_store: DeadLetterQueueStore,
        event_repository: EventRepository | None = None,
        connection_manager: ConnectionManager | None = None,
    ) -> None:
        self.settings = settings
        self.event_processor = TransactionEventProcessor(
            event_store,
            live_metrics,
            ops_metrics,
            event_repository,
            connection_manager,
        )
        self.ops_metrics = ops_metrics
        self.dlq_store = dlq_store
        self.connection_manager = connection_manager
        self._consumer: KafkaConsumer | None = None
        self._stop_event = Event()
        self._thread: Thread | None = None

    def start(self) -> None:
        if not self.settings.enable_event_consumer:
            logger.info("Transaction event consumer is disabled.")
            return

        self._thread = Thread(target=self._consume_loop, name="transaction-event-consumer", daemon=True)
        self._thread.start()
        logger.info(
            "Starting transaction event consumer for topic '%s' on brokers '%s'.",
            self.settings.event_topic,
            self.settings.kafka_brokers,
        )

    def stop(self) -> None:
        self._stop_event.set()

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)

        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None

    def _consume_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                if self._consumer is None:
                    self._consumer = self._build_consumer()
                    logger.info(
                        "Transaction event consumer connected to topic '%s'.",
                        self.settings.event_topic,
                    )

                for message in self._consumer:
                    if self._stop_event.is_set():
                        break

                    self._handle_message(message.value)

                    if self._stop_event.is_set():
                        break
            except KafkaError:
                logger.exception("Kafka consumer error while processing transaction events.")
                self._reset_consumer()
                self._stop_event.wait(5)
            except Exception:
                logger.exception("Unexpected error while processing transaction events.")
                self._stop_event.wait(2)

    def _build_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            self.settings.event_topic,
            bootstrap_servers=self.settings.kafka_brokers.split(","),
            group_id=self.settings.kafka_consumer_group,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=1000,
        )

    def _handle_message(self, payload: object) -> None:
        try:
            event = TransactionEvent.model_validate(payload)
        except ValidationError as error:
            self._handle_invalid_payload(payload, error)
            return

        self.event_processor.process(event)

    def _reset_consumer(self) -> None:
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None

    def _handle_invalid_payload(self, payload: object, error: ValidationError) -> None:
        payload_for_dlq = payload if isinstance(payload, dict) else {"raw_payload": payload}
        error_message = "; ".join(
            f"{'.'.join(str(part) for part in item['loc'])}: {item['msg']}"
            for item in error.errors()
        )
        dlq_item = self.dlq_store.record(payload_for_dlq, error_message)
        ops = self.ops_metrics.record_invalid_event(dlq_item.received_at)
        logger.warning("Invalid transaction event payload=%s errors=%s", payload_for_dlq, error_message)

        if self.connection_manager is not None:
            self.connection_manager.publish_operational_update(
                ops,
                self.dlq_store.recent(limit=5),
            )
