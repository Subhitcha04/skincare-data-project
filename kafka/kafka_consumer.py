"""
GlowCheck — Apache Kafka Consumer
Consumes ingredient safety alerts from the Kafka topic,
applies real-time filtering for CRITICAL alerts,
and triggers backfill if a high-volume ingredient is detected.

Topic: glowcheck-safety-alerts
"""
import json
import time
from datetime import datetime

try:
    from kafka import KafkaConsumer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

TOPIC     = "glowcheck-safety-alerts"
BOOTSTRAP = "localhost:9092"
GROUP_ID  = "glowcheck-alert-processor"

CRITICAL_THRESHOLD = 100   # event_count above this triggers backfill


def process_alert(alert: dict):
    """
    Process one alert message:
    - Log all alerts
    - Flag CRITICAL risk alerts
    - Trigger backfill if event count exceeds threshold
    """
    ingredient  = alert.get("ingredient", "unknown")
    risk_level  = alert.get("risk_level", "UNKNOWN")
    event_count = alert.get("event_count", 0)
    timestamp   = alert.get("timestamp", "")

    print(f"[{timestamp}] RECEIVED → {ingredient} | risk={risk_level} | events={event_count}")

    if risk_level == "CRITICAL":
        print(f"  ⚠ CRITICAL ALERT: {ingredient} flagged for immediate review")

    if event_count > CRITICAL_THRESHOLD:
        print(
            f"  ↺ BACKFILL TRIGGERED: {ingredient} has {event_count} events "
            f"— scheduling historical replay"
        )


def run_consumer(max_messages: int = 20):
    if not KAFKA_AVAILABLE:
        print("kafka-python not installed — running in simulation mode")
        print("Simulating consumption of Kafka messages:")
        print("-" * 60)

        import random
        ingredients = ["hydroquinone", "mercury", "formaldehyde",
                       "tretinoin", "clobetasol", "lead acetate"]
        risk_levels = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]

        for i in range(max_messages):
            alert = {
                "ingredient":  random.choice(ingredients),
                "risk_level":  random.choice(risk_levels),
                "event_count": random.randint(1, 500),
                "timestamp":   datetime.utcnow().isoformat(),
            }
            process_alert(alert)
            time.sleep(0.3)
        return

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=10000,
    )

    print(f"Connected to Kafka at {BOOTSTRAP}")
    print(f"Consuming from topic: {TOPIC} | group: {GROUP_ID}")
    print("-" * 60)

    count = 0
    for message in consumer:
        process_alert(message.value)
        count += 1
        if count >= max_messages:
            break

    consumer.close()
    print(f"\nConsumed {count} messages.")


if __name__ == "__main__":
    run_consumer()