import json
import random
import time
from datetime import datetime, timezone
from uuid import uuid4

from kafka import KafkaProducer


TOPIC = "fintrust.transactions"
BOOTSTRAP_SERVERS = "localhost:9092"


CUSTOMERS = ["C001", "C002", "C003", "C004"]
MERCHANTS = ["M001", "M002", "M003", "M004"]
CHANNELS = ["POS", "ONLINE", "ATM"]
COUNTRIES = ["CA", "US", "UK", "AE"]
CURRENCIES = ["CAD", "USD", "GBP"]
STATUSES = ["APPROVED", "DECLINED"]


def generate_transaction() -> dict:
    return {
        "transaction_id": f"T-{uuid4()}",
        "customer_id": random.choice(CUSTOMERS),
        "card_id": f"CARD-{random.randint(100, 999)}",
        "merchant_id": random.choice(MERCHANTS),
        "amount": round(random.uniform(5, 5000), 2),
        "currency": random.choice(CURRENCIES),
        "transaction_timestamp": datetime.now(timezone.utc).isoformat(),
        "channel": random.choice(CHANNELS),
        "location_country": random.choice(COUNTRIES),
        "transaction_status": random.choice(STATUSES),
    }


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    print(f"Producing transaction events to topic: {TOPIC}")

    while True:
        event = generate_transaction()
        producer.send(TOPIC, event)
        producer.flush()

        print(event)
        time.sleep(2)


if __name__ == "__main__":
    main()