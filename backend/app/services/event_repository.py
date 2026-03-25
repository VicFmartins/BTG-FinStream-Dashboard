from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import desc, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from app.models.transaction_event import PersistedTransactionEvent
from app.schemas.transaction_event import HistoricalTransactionEvent, TransactionEvent


def parse_event_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class EventRepository:
    def __init__(self, session_factory: sessionmaker) -> None:
        self._session_factory = session_factory

    def save(self, event: TransactionEvent) -> bool:
        with self._session_factory() as session:
            persisted_event = PersistedTransactionEvent(
                event_id=event.event_id,
                client_id=event.client_id,
                asset=event.asset,
                event_type=event.event_type,
                amount=event.amount,
                unit_price=event.unit_price,
                quantity=event.quantity,
                notional_amount=event.notional_amount,
                timestamp=parse_event_timestamp(event.timestamp),
                ingested_at=datetime.now(UTC),
            )
            session.add(persisted_event)

            try:
                session.commit()
                return True
            except IntegrityError:
                session.rollback()
                return False

    def history(
        self,
        limit: int,
        client_id: str | None = None,
        asset: str | None = None,
        event_type: str | None = None,
    ) -> list[HistoricalTransactionEvent]:
        with self._session_factory() as session:
            statement = select(PersistedTransactionEvent)

            if client_id:
                statement = statement.where(PersistedTransactionEvent.client_id == client_id)
            if asset:
                statement = statement.where(PersistedTransactionEvent.asset == asset.upper())
            if event_type:
                statement = statement.where(PersistedTransactionEvent.event_type == event_type.upper())

            statement = statement.order_by(
                desc(PersistedTransactionEvent.timestamp),
                desc(PersistedTransactionEvent.ingested_at),
            ).limit(limit)

            events = session.scalars(statement).all()
            return [self._to_schema(item) for item in events]

    def _to_schema(self, item: PersistedTransactionEvent) -> HistoricalTransactionEvent:
        amount = float(item.amount) if isinstance(item.amount, Decimal) else item.amount
        quantity = item.quantity or 1
        unit_price = (
            float(item.unit_price)
            if isinstance(item.unit_price, Decimal)
            else item.unit_price
        )
        notional_amount = (
            float(item.notional_amount)
            if isinstance(item.notional_amount, Decimal)
            else item.notional_amount
        )

        unit_price = round(unit_price if unit_price is not None else amount / quantity, 4)
        notional_amount = round(notional_amount if notional_amount is not None else amount, 2)

        return HistoricalTransactionEvent(
            event_id=item.event_id,
            client_id=item.client_id,
            asset=item.asset,
            event_type=item.event_type,
            amount=notional_amount,
            unit_price=unit_price,
            quantity=quantity,
            notional_amount=notional_amount,
            timestamp=item.timestamp.isoformat(),
            ingested_at=item.ingested_at.isoformat(),
        )
