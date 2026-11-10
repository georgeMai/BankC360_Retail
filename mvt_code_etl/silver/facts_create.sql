CREATE SCHEMA IF NOT EXISTS lakehouse.silver;

-- =============================================================================
-- 1. BẢNG FACT_ACCOUNT_TXN (Transaction Fact - Giao dịch tài khoản)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.silver.fact_account_txn (
    txn_id          BIGINT,
    account_id      BIGINT,
    customer_id     BIGINT,
    txn_date        TIMESTAMP,
    txn_amount      DECIMAL(18,2),
    txn_type        STRING,
    debit_credit    STRING,
    balance_after   DECIMAL(18,2),
    channel         STRING,
    description     STRING,
    counter_account STRING,
    created_ts      TIMESTAMP,
    last_updated    TIMESTAMP,
    cob_dt          DATE
)
USING iceberg
PARTITIONED BY (days(txn_date))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '268435456'
);

-- =============================================================================
-- 2. BẢNG FACT_CARD_TXN (Transaction Fact - Giao dịch thẻ)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.silver.fact_card_txn (
    txn_id            BIGINT,
    card_id           BIGINT,
    customer_id       BIGINT,
    txn_date          TIMESTAMP,
    txn_amount        DECIMAL(18,2),
    txn_type          STRING,
    currency          STRING,
    merchant_name     STRING,
    merchant_category STRING,
    channel           STRING,
    status            STRING,
    created_ts        TIMESTAMP,
    last_updated      TIMESTAMP,
    cob_dt            DATE
)
USING iceberg
PARTITIONED BY (days(txn_date))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '268435456'
);

-- =============================================================================
-- 3. BẢNG FACT_CRM_INTERACTION (Event Fact - Tương tác CRM)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.silver.fact_crm_interaction (
    interaction_id   BIGINT,
    customer_id      BIGINT,
    interaction_date TIMESTAMP,
    channel          STRING,
    direction        STRING,
    subject          STRING,
    category         STRING,
    status           STRING,
    assigned_to      STRING,
    created_ts       TIMESTAMP,
    last_updated     TIMESTAMP,
    cob_dt          DATE
)
USING iceberg
PARTITIONED BY (days(interaction_date))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- =============================================================================
-- 4. BẢNG FACT_DEPOSIT_SNAPSHOT (Periodic Snapshot Fact - Trạng thái Tiết kiệm)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.silver.fact_deposit_snapshot (
    deposit_id       BIGINT,
    account_id       BIGINT,
    customer_id      BIGINT,
    product_code     STRING,
    principal_amount DECIMAL(18,2),
    interest_rate    DECIMAL(5,2),
    term_months      INT,
    open_date        DATE,
    maturity_date    DATE,
    status           STRING,
    last_updated     TIMESTAMP,
    cob_dt           DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- =============================================================================
-- 5. BẢNG FACT_LOAN_SNAPSHOT (Periodic Snapshot Fact - Trạng thái Khoản vay)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.silver.fact_loan_snapshot (
    loan_id             BIGINT,
    customer_id         BIGINT,
    product_code        STRING,
    branch_code         STRING,
    loan_amount         DECIMAL(18,2),
    outstanding_balance DECIMAL(18,2),
    interest_rate       DECIMAL(5,2),
    term_months         INT,
    disbursement_date   DATE,
    maturity_date       DATE,
    loan_status         STRING,
    last_updated        TIMESTAMP,
    cob_dt              DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);