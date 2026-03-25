from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class TransactionEvent(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    event_id: str = Field(min_length=1)
    client_id: str = Field(min_length=1)
    asset: str = Field(min_length=1)
    event_type: Literal["BUY", "SELL", "DEPOSIT", "WITHDRAWAL"]
    amount: float = Field(gt=0)
    unit_price: float | None = Field(default=None, gt=0)
    quantity: int | None = Field(default=None, gt=0)
    notional_amount: float | None = Field(default=None, gt=0)
    timestamp: str = Field(min_length=1)

    @field_validator("asset")
    @classmethod
    def normalize_asset(cls, value: str) -> str:
        return value.upper()

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, value: str) -> str:
        if "T" not in value:
            raise ValueError("timestamp must be an ISO 8601 datetime string")

        return value

    @model_validator(mode="after")
    def populate_financial_fields(self) -> "TransactionEvent":
        quantity = self.quantity or 1
        unit_price = self.unit_price or round(self.amount / quantity, 4)
        notional_amount = self.notional_amount or round(unit_price * quantity, 2)

        self.quantity = quantity
        self.unit_price = round(unit_price, 4)
        self.notional_amount = round(notional_amount, 2)
        self.amount = self.notional_amount
        return self


class HistoricalTransactionEvent(TransactionEvent):
    ingested_at: str


class LiveMetricsSnapshot(BaseModel):
    total_processed_events: int
    total_transaction_volume: float
    events_by_type: dict[str, int]
    latest_client_id: str | None
    latest_asset: str | None
    latest_event_timestamp: str | None


class EventStreamStatus(BaseModel):
    consumer_enabled: bool
    kafka_brokers: str
    topic: str
    latest_event: TransactionEvent | None
    metrics: LiveMetricsSnapshot
