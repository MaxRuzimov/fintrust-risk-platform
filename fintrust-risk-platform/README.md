# FinTrust Enterprise Data Platform

## Architecture

FinTrust is an enterprise data platform built around Azure Databricks, Delta Lake, Unity Catalog, Event Hub, ADLS-style landing zones, and later Snowflake as the analytics serving layer.

## Data Flow

### Streaming Transactions

```text
Transaction Producer API
        ↓
Azure Event Hub
        ↓
Databricks Bronze
        ↓
Databricks Silver
        ↓
Quarantine
        ↓
Databricks Gold