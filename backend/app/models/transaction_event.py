from datetime import datetime

from sqlalchemy import DateTime, Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class PersistedTransactionEvent(Base):
    __tablename__ = "transaction_events"

    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    client_id: Mapped[str] = mapped_column(String(64), index=True)
    asset: Mapped[str] = mapped_column(String(32), index=True)
    event_type: Mapped[str] = mapped_column(String(32), index=True)
    amount: Mapped[float] = mapped_column(Numeric(18, 2))
    unit_price: Mapped[float | None] = mapped_column(Numeric(18, 4), nullable=True)
    quantity: Mapped[int | None] = mapped_column(Integer, nullable=True)
    notional_amount: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
