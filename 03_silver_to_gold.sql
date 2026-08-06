-- Banking Medallion Architecture: Silver to Gold Aggregation
-- File: 03_silver_to_gold.sql

CREATE TABLE banking_catalog.gold.daily_customer_balance_summary AS
WITH temp_daily_summary AS (
    SELECT 
        customer_id,
        DATE(transaction_timestamp) as txn_date,
        SUM(amount) as total_deposit_amount,
        COUNT(transaction_id) as total_txn_count,
        AVG(amount) as avg_txn_value
    FROM banking_catalog.silver.cleansed_transactions
    WHERE transaction_status = 'COMPLETED'
    GROUP BY customer_id, DATE(transaction_timestamp)
)
SELECT 
    s.customer_id,
    c.customer_name,
    c.risk_segment,
    s.txn_date,
    s.total_deposit_amount,
    s.total_txn_count,
    s.avg_txn_value,
    CURRENT_TIMESTAMP() as processed_at
FROM temp_daily_summary s
LEFT OUTER JOIN banking_catalog.silver.verified_customer_accounts c 
    ON s.customer_id = c.customer_id;

-- High Risk Account Alert Aggregation Table
INSERT INTO banking_catalog.gold.high_risk_alerts
SELECT 
    customer_id,
    txn_date,
    total_deposit_amount,
    'FLAGGED_HIGH_VOLUME' as alert_reason
FROM banking_catalog.gold.daily_customer_balance_summary
WHERE total_deposit_amount > 50000;
