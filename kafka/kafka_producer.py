"""
GlowCheck — Apache Kafka Producer
Streams ingredient safety alerts to a Kafka topic in real time.
Each message represents a high-risk adverse event detected by the pipeline.

Topic: glowcheck-safety-alerts
Message format: JSON with ingredient, risk_level, event_count, timestamp
"""
import json
import time
import random
from datetime import datetime

try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

TOPIC     = "glowcheck-safety-alerts"
BOOTSTRAP = "localhost:9092"

HIGH_RISK_INGREDIENTS = [
    "hydroquinone", "mercury", "lead acetate",
    "formaldehyde", "tretinoin", "clobetasol"
]

RISK_LEVELS = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]


def generate_alert(ingredient: str) -> dict:
    return {
        "ingredient":    ingredient,
        "risk_level":    random.choice(RISK_LEVELS),
        "event_count":   random.randint(1, 500),
        "country":       random.choice(["US", "IN", "DE", "JP", "GB"]),
        "source":        "openFDA",
        "timestamp":     datetime.utcnow().isoformat(),
        "pipeline_run":  "glowcheck_daily",
    }


def run_producer(num_messages: int = 20, delay: float = 0.5):
    if not KAFKA_AVAILABLE:
        print("kafka-python not installed — running in simulation mode")
        print("In production: pip install kafka-python and start Kafka broker")
        print()
        print("Simulated Kafka messages:")
        print("-" * 60)
        for i in range(num_messages):
            ingredient = random.choice(HIGH_RISK_INGREDIENTS)
            alert      = generate_alert(ingredient)
            print(f"[MSG {i+1:02d}] Topic: {TOPIC}")
            print(f"         {json.dumps(alert, indent=2)}")
            time.sleep(delay)
        return

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",              # wait for all replicas to confirm
        retries=3,
        retry_backoff_ms=500,
    )

    print(f"Connected to Kafka at {BOOTSTRAP}")
    print(f"Streaming {num_messages} alerts to topic: {TOPIC}")
    print("-" * 60)

    for i in range(num_messages):
        ingredient = random.choice(HIGH_RISK_INGREDIENTS)
        alert      = generate_alert(ingredient)
        future     = producer.send(TOPIC, value=alert)
        metadata   = future.get(timeout=10)
        print(
            f"[{i+1:02d}] Sent → partition={metadata.partition} "
            f"offset={metadata.offset} | {alert['ingredient']} "
            f"| risk={alert['risk_level']}"
        )
        time.sleep(delay)

    producer.flush()
    producer.close()
    print("\nProducer done.")


if __name__ == "__main__":
    run_producer()