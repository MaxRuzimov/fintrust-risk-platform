import json
import os
import random
import time
from datetime import datetime, timezone
from uuid import uuid4

from azure.eventhub import EventData, EventHubProducerClient


CONNECTION_STRING = os.getenv("EVENTHUB_CONNECTION_STRING")
EVENTHUB_NAME = os.getenv("EVENTHUB_NAME", "fintrust-transactions")

CUSTOMERS = ["C001", "C002", "C003", "C004"]
MERCHANTS = ["M001", "M002", "M003", "M004"]

INVALID_TEMPLATES = [
    lambda: {"transaction_id": None, "customer_id": random.choice(CUSTOMERS), "merchant_id": random.choice(MERCHANTS), "amount": round(random.uniform(5, 5000), 2), "currency": random.choice(["CAD", "USD", "GBP"])},
    lambda: {"transaction_id": f"T-{uuid4()}", "customer_id": None, "merchant_id": random.choice(MERCHANTS), "amount": round(random.uniform(5, 5000), 2), "currency": random.choice(["CAD", "USD", "GBP"])},
    lambda: {"transaction_id": f"T-{uuid4()}", "customer_id": random.choice(CUSTOMERS), "merchant_id": random.choice(MERCHANTS), "amount": round(random.uniform(-500, -1), 2), "currency": random.choice(["CAD", "USD", "GBP"])},
    lambda: {"transaction_id": f"T-{uuid4()}", "customer_id": random.choice(CUSTOMERS), "merchant_id": random.choice(MERCHANTS), "amount": round(random.uniform(5, 5000), 2), "currency": random.choice(["BITCOIN", "DOGECOIN", "XYZ"])},
]


def generate_transaction() -> dict:
    return {
        "transaction_id": f"T-{uuid4()}",
        "customer_id": random.choice(CUSTOMERS),
        "card_id": f"CARD-{random.randint(100, 999)}",
        "merchant_id": random.choice(MERCHANTS),
        "amount": round(random.uniform(5, 5000), 2),
        "currency": random.choice(["CAD", "USD", "GBP"]),
        "transaction_timestamp": datetime.now(timezone.utc).isoformat(),
        "channel": random.choice(["POS", "ONLINE", "ATM"]),
        "location_country": random.choice(["CA", "US", "UK", "AE"]),
        "transaction_status": random.choice(["APPROVED", "DECLINED"]),
    }


def generate_invalid_transaction() -> dict:
    return random.choice(INVALID_TEMPLATES)()


def main() -> None:
    if not CONNECTION_STRING:
        raise ValueError("EVENTHUB_CONNECTION_STRING environment variable is missing")

    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        eventhub_name=EVENTHUB_NAME,
    )

    with producer:
        while True:
            # ~1 in 5 events is invalid
            event = generate_invalid_transaction() if random.random() < 0.2 else generate_transaction()
            batch = producer.create_batch()
            batch.add(EventData(json.dumps(event)))
            producer.send_batch(batch)
            print(event)
            time.sleep(2)


if __name__ == "__main__":
    main()