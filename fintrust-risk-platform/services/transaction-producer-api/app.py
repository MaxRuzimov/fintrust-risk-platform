import json
import os
import random
from datetime import datetime, timezone
from uuid import uuid4

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from fastapi import FastAPI
from pydantic import BaseModel


app = FastAPI(title="FinTrust Transaction Producer API")


EVENTHUB_CONNECTION_STRING = os.getenv("EVENTHUB_CONNECTION_STRING")
EVENTHUB_NAME = os.getenv("EVENTHUB_NAME", "fintrust-transactions")


class TransactionRequest(BaseModel):
    customer_id: str
    card_id: str
    merchant_id: str
    amount: float
    currency: str
    channel: str
    location_country: str
    transaction_status: str


@app.get("/health")
def health_check():
    return {"status": "healthy"}


@app.post("/transactions")
async def publish_transaction(request: TransactionRequest):
    event = {
        "transaction_id": f"T-{uuid4()}",
        "customer_id": request.customer_id,
        "card_id": request.card_id,
        "merchant_id": request.merchant_id,
        "amount": request.amount,
        "currency": request.currency,
        "transaction_timestamp": datetime.now(timezone.utc).isoformat(),
        "channel": request.channel,
        "location_country": request.location_country,
        "transaction_status": request.transaction_status,
    }

    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENTHUB_CONNECTION_STRING,
        eventhub_name=EVENTHUB_NAME,
    )

    async with producer:
        batch = await producer.create_batch()
        batch.add(EventData(json.dumps(event)))
        await producer.send_batch(batch)

    return {
        "message": "transaction_published",
        "event": event,
    }


@app.post("/transactions/random")
async def publish_random_transaction():
    request = TransactionRequest(
        customer_id=random.choice(["C001", "C002", "C003", "C004"]),
        card_id=f"CARD-{random.randint(100, 999)}",
        merchant_id=random.choice(["M001", "M002", "M003", "M004"]),
        amount=round(random.uniform(5, 5000), 2),
        currency=random.choice(["CAD", "USD", "GBP"]),
        channel=random.choice(["POS", "ONLINE", "ATM"]),
        location_country=random.choice(["CA", "US", "UK", "AE"]),
        transaction_status=random.choice(["APPROVED", "DECLINED"]),
    )

    return await publish_transaction(request)