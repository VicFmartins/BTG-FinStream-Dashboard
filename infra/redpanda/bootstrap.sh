#!/bin/sh
set -e

TOPIC_NAME="${EVENT_TOPIC:-${MARKET_TOPIC:-transactions.events}}"

until rpk cluster info --brokers redpanda:9092 >/dev/null 2>&1; do
  echo "waiting for redpanda..."
  sleep 2
done

rpk topic create "$TOPIC_NAME" --brokers redpanda:9092 || true
echo "topic ensured: $TOPIC_NAME"
