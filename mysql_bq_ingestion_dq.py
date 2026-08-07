-- Step 1: Create Foreign Connection to MySQL
CREATE CONNECTION IF NOT EXISTS mysql_recon_conn TYPE MYSQL
OPTIONS (
  host '127.0.0.1',
  port '3306',
  user 'someusr',
  password 'somepasswd'
);

-- Step 2: Create Foreign Catalog
CREATE FOREIGN CATALOG IF NOT EXISTS mysql_foreign_catalog
USING CONNECTION mysql_recon_conn
OPTIONS (database 'recon_db');

-- Step 3: Query Foreign Catalog directly in PySpark / SQL

CREATE TABLE recon_dataset.settlement_transactions_dq AS
SELECT
    settlement_id,
    transaction_id,
    settlement_date,
    value_date,
    settled_amount,
    currency,
    settlement_status,
    bank_ref_id,
    settlement_batch_id,
    bank_name,
    ifsc_code,
    neft_utr,
    remarks,
    created_at,
    updated_at,
    current_timestamp() AS dw_ingested_at
FROM mysql_foreign_catalog.recon_db.gl_transactions;
