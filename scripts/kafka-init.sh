#!/bin/sh
set -e
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"

TOPICS="harvest-events dataset-events concept-events data-service-events information-model-events service-events event-events"

for topic in $TOPICS; do
  echo "Creating topic: $topic"
  kafka-topics --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$topic" \
    --replication-factor 1 \
    --partitions 1
done

echo "All topics created."
