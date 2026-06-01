Workflow: fintrust_streaming_transactions

Task 1:
01_loaders/01_transactions_eventhub_to_bronze.py

Task 2:
02_transform_silver/01_transactions_to_silver.py

Task 3:
02_transform_silver/02_transactions_quarantine.py

Workflow: fintrust_customer_batch

Task 1:
01_loaders/02_customers_autoloader_to_bronze.py

Task 2:
02_transform_silver/02_customers_scd2.py

Task 3:
04_quality_audit/01_reconciliation_checks.py


Workflow: fintrust_exchange_rates_batch

Task 1:
01_loaders/03_exchange_rates_autoloader_to_bronze.py

Task 2:
02_transform_silver/03_exchange_rates_to_silver.py


Workflow: fintrust_gold_refresh

Task 1:
03_transform_gold/01_transactions_enriched.py

Task 2:
04_quality_audit/01_reconciliation_checks.py