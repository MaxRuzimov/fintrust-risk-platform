TRANSACTIONS_CONTRACT = {
    "transaction_id": "string",
    "customer_id": "string",
    "card_id": "string",
    "merchant_id": "string",
    "amount": "double",
    "currency": "string",
    "transaction_timestamp": "string",
    "channel": "string",
    "location_country": "string",
    "transaction_status": "string",
    "ingestion_timestamp": "timestamp",
}

CUSTOMERS_CONTRACT = {
    "customer_id": "string",
    "customer_name": "string",
    "customer_country": "string",
    "customer_risk_tier": "string",
    "kyc_status": "string",
    "effective_date": "string",
    "source_file": "string",
    "ingestion_timestamp": "timestamp",
    "load_id": "string",
}

EXCHANGE_RATES_CONTRACT = {
    "currency": "string",
    "rate_date": "string",
    "rate_to_cad": "string",
    "source_file": "string",
    "ingestion_timestamp": "timestamp",
    "load_id": "string",
}
