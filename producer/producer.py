import json
import os
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from uuid import uuid4

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:19092")
EVENT_TOPIC = os.getenv("EVENT_TOPIC", os.getenv("MARKET_TOPIC", "transactions.events"))
PUBLISH_INTERVAL_SECONDS = float(os.getenv("PUBLISH_INTERVAL_SECONDS", "1.5"))
MAX_EVENTS = int(os.getenv("MAX_EVENTS", "0"))
ENABLE_KAFKA = os.getenv("ENABLE_KAFKA", "true").lower() == "true"
KAFKA_STARTUP_TIMEOUT_SECONDS = int(os.getenv("KAFKA_STARTUP_TIMEOUT_SECONDS", "30"))
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "").strip()
FINNHUB_QUOTE_URL = os.getenv("FINNHUB_QUOTE_URL", "https://finnhub.io/api/v1/quote")
FINNHUB_TIMEOUT_SECONDS = float(os.getenv("FINNHUB_TIMEOUT_SECONDS", "10"))
FINNHUB_SYMBOLS = [
    symbol.strip().upper()
    for symbol in os.getenv("FINNHUB_SYMBOLS", "AAPL,MSFT,NVDA,GOOGL,AMZN").split(",")
    if symbol.strip()
]

CLIENT_IDS = [f"client-{index:04d}" for index in range(1, 26)]
EVENT_TYPES = ["BUY", "SELL", "DEPOSIT", "WITHDRAWAL"]

_missing_key_warning_emitted = False


def wait_for_redpanda() -> None:
    deadline = time.time() + KAFKA_STARTUP_TIMEOUT_SECONDS

    while True:
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKERS.split(","))
            admin_client.close()
            return
        except NoBrokersAvailable:
            if time.time() >= deadline:
                raise

            print("waiting for Redpanda...")
            time.sleep(2)


def ensure_topic() -> None:
    topic = NewTopic(name=EVENT_TOPIC, num_partitions=1, replication_factor=1)
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKERS.split(","))

    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"created topic: {EVENT_TOPIC}")
    except TopicAlreadyExistsError:
        print(f"topic already exists: {EVENT_TOPIC}")
    finally:
        admin_client.close()


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS.split(","),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        linger_ms=50,
        retries=5,
    )


def fetch_finnhub_price(symbol: str) -> float | None:
    global _missing_key_warning_emitted

    if not FINNHUB_API_KEY:
        if not _missing_key_warning_emitted:
            print("FINNHUB_API_KEY not set. Falling back to locally simulated pricing.")
            _missing_key_warning_emitted = True
        return None

    query = urllib.parse.urlencode({"symbol": symbol, "token": FINNHUB_API_KEY})
    request = urllib.request.Request(
        f"{FINNHUB_QUOTE_URL}?{query}",
        headers={
            "Accept": "application/json",
            "User-Agent": "BTG-FinStream-Producer/1.0",
        },
    )

    try:
        with urllib.request.urlopen(request, timeout=FINNHUB_TIMEOUT_SECONDS) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (
        urllib.error.HTTPError,
        urllib.error.URLError,
        TimeoutError,
        json.JSONDecodeError,
    ) as error:
        print(f"finnhub quote request failed for {symbol}: {error}")
        return None

    price = payload.get("c")
    if not isinstance(price, (int, float)) or price <= 0:
        print(f"finnhub quote missing usable current price for {symbol}: {payload}")
        return None

    return float(price)


def fallback_price(symbol: str) -> float:
    seed = sum(ord(character) for character in symbol)
    randomizer = random.Random(f"{seed}-{datetime.now(UTC).strftime('%Y%m%d%H%M')}")
    return round(randomizer.uniform(25.0, 350.0), 2)


def derive_quantity(event_type: str) -> int:
    if event_type in {"BUY", "SELL"}:
        return random.randint(10, 150)

    return random.randint(100, 1500)


def financial_transaction_event() -> tuple[dict[str, object], str, float]:
    symbol = random.choice(FINNHUB_SYMBOLS)
    event_type = random.choice(EVENT_TYPES)
    current_price = fetch_finnhub_price(symbol)
    price_source = "finnhub"

    if current_price is None:
        current_price = fallback_price(symbol)
        price_source = "fallback"

    quantity = derive_quantity(event_type)
    unit_price = round(current_price, 4)
    notional_amount = round(unit_price * quantity, 2)

    event = {
        "event_id": str(uuid4()),
        "client_id": random.choice(CLIENT_IDS),
        "asset": symbol,
        "event_type": event_type,
        "amount": notional_amount,
        "unit_price": unit_price,
        "quantity": quantity,
        "notional_amount": notional_amount,
        "timestamp": datetime.now(UTC).isoformat(),
    }
    return event, price_source, current_price


def publish_event(
    producer: KafkaProducer | None,
    event: dict[str, object],
    price_source: str,
    current_price: float,
) -> None:
    if producer is None:
        print(
            json.dumps(
                {
                    "source": price_source,
                    "price": current_price,
                    "event": event,
                }
            )
        )
        return

    producer.send(EVENT_TOPIC, event)
    producer.flush()
    print(
        f"published to {EVENT_TOPIC} source={price_source} price={current_price:.2f}: "
        f"{json.dumps(event)}"
    )


def main() -> None:
    if ENABLE_KAFKA:
        wait_for_redpanda()
        ensure_topic()
        producer = build_producer()
    else:
        producer = None

    sent_events = 0

    try:
        while True:
            event, price_source, current_price = financial_transaction_event()
            publish_event(producer, event, price_source, current_price)
            sent_events += 1

            if MAX_EVENTS and sent_events >= MAX_EVENTS:
                break

            time.sleep(PUBLISH_INTERVAL_SECONDS)
    finally:
        if producer is not None:
            producer.close()


if __name__ == "__main__":
    main()
