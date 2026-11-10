CREATE SCHEMA IF NOT EXISTS lakehouse.bronze;

-- =============================================================================
-- 1. NHÓM BẢNG TỪ CORE BANKING (ORACLE XE 21C)
-- =============================================================================

-- 1.1 Chi nhánh ngân hàng (Full Snapshot)
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_branch (
    branch_code  STRING,
    branch_name  STRING,
    region       STRING,
    city         STRING,
    district     STRING,
    address      STRING,
    manager_name STRING,
    open_date    DATE,
    status       STRING,
    last_updated TIMESTAMP,
    cob_dt       DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- 1.2 Danh mục sản phẩm (Full Snapshot)
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_product (
    product_code  STRING,
    product_name  STRING,
    product_group STRING,
    product_type  STRING,
    currency      STRING,
    is_active     INT,
    launch_date   DATE,
    last_updated  TIMESTAMP,
    cob_dt        DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- 1.3 Khách hàng (Full Snapshot)
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_customer (
    customer_id      BIGINT,
    cccd             STRING,
    full_name        STRING,
    gender           STRING,
    date_of_birth    DATE,
    phone            STRING,
    email            STRING,
    address          STRING,
    city             STRING,
    district         STRING,
    branch_code      STRING,
    customer_segment STRING,
    kyc_status       STRING,
    register_date    DATE,
    is_active        INT,
    last_updated     TIMESTAMP,
    cob_dt           DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- 1.4 Tài khoản thanh toán / CASA (Full Snapshot)
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_account (
    account_id   BIGINT,
    account_no   STRING,
    customer_id  BIGINT,
    product_code STRING,
    branch_code  STRING,
    account_type STRING,
    currency     STRING,
    balance      DECIMAL(18,2),
    open_date    DATE,
    close_date   DATE,
    status       STRING,
    last_updated TIMESTAMP,
    cob_dt       DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- 1.5 Sổ tiết kiệm (Full Snapshot)
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_deposit (
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

-- 1.6 Khoản vay (Full Snapshot)
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_loan (
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

-- 1.7 Giao dịch tài khoản (Incremental)
CREATE TABLE IF NOT EXISTS lakehouse.bronze.core_txn_account (
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
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '268435456'
);


-- =============================================================================
-- 2. NHÓM BẢNG TỪ CARD & CRM (POSTGRESQL 15)
-- =============================================================================

-- 2.1 Danh mục thẻ (Full Snapshot)
CREATE TABLE IF NOT EXISTS lakehouse.bronze.card (
    card_id        BIGINT,
    card_no_masked STRING,
    customer_id    BIGINT,
    account_id     BIGINT,
    product_code   STRING,
    card_type      STRING,
    card_brand     STRING,
    credit_limit   DECIMAL(18,2),
    issue_date     DATE,
    expiry_date    DATE,
    status         STRING,
    last_updated   TIMESTAMP,
    cob_dt         DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- 2.2 Giao dịch thẻ (Incremental)
CREATE TABLE IF NOT EXISTS lakehouse.bronze.card_txn (
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
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '268435456'
);

-- 2.3 Tương tác CRM (Incremental)
CREATE TABLE IF NOT EXISTS lakehouse.bronze.crm_interaction (
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
    cob_dt           DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);