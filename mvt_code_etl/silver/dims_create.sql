-- Create Catalog / Schema if not exists
CREATE SCHEMA IF NOT EXISTS lakehouse.silver;

-- =============================================================================
-- 1. BẢNG DIM_CUSTOMER (SCD TYPE 2)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.silver.dim_customer (
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
    -- Cột quản lý SCD Type 2
    effective_from   DATE,
    effective_to     DATE,
    is_current       INT
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- =============================================================================
-- 2. BẢNG DIM_ACCOUNT (SCD TYPE 2)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.silver.dim_account (
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
    -- Cột quản lý SCD Type 2
    effective_from DATE,
    effective_to   DATE,
    is_current     INT
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- =============================================================================
-- 3. BẢNG DIM_PRODUCT (SCD TYPE 2)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.silver.dim_product (
    product_code  STRING,
    product_name  STRING,
    product_group STRING,
    product_type  STRING,
    currency      STRING,
    is_active     INT,
    launch_date   DATE,
    last_updated  TIMESTAMP,
    -- Cột quản lý SCD Type 2
    effective_from DATE,
    effective_to   DATE,
    is_current     INT
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- =============================================================================
-- 4. BẢNG DIM_BRANCH (SCD TYPE 2)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.silver.dim_branch (
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
    -- Cột quản lý SCD Type 2
    effective_from DATE,
    effective_to   DATE,
    is_current     INT
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- =============================================================================
-- 5. BẢNG DIM_CARD (SCD TYPE 2)
-- =============================================================================
CREATE TABLE IF NOT EXISTS lakehouse.silver.dim_card (
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
    -- Cột quản lý SCD Type 2
    effective_from DATE,
    effective_to   DATE,
    is_current     INT
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);