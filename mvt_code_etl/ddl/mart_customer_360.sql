CREATE SCHEMA IF NOT EXISTS lakehouse.gold;

CREATE TABLE IF NOT EXISTS lakehouse.gold.mart_customer_360 (
    -- Nhóm Demographics & Profile
    customer_id                     BIGINT,
    customer_age                    INT,
    tenure_months                   INT,
    customer_segment                STRING,
    kyc_status                      STRING,
    
    -- Nhóm CASA & Tiền gửi
    total_casa_balance              DECIMAL(18,2),
    count_active_accounts           INT,
    total_deposit_amount            DECIMAL(18,2),
    count_active_deposits           INT,
    deposit_maturing_next_30d_amount DECIMAL(18,2),
    
    -- Nhóm Tín dụng & Khoản vay
    total_loan_outstanding          DECIMAL(18,2),
    count_active_loans              INT,
    has_overdue_loan                INT,
    max_loan_interest_rate          DECIMAL(5,2),
    loan_maturing_next_60d_amount   DECIMAL(18,2),
    
    -- Nhóm Thẻ
    count_active_credit_cards       INT,
    count_active_debit_cards        INT,
    total_credit_limit              DECIMAL(18,2),
    card_spend_amount_30d           DECIMAL(18,2),
    
    -- Nhóm Hành vi giao dịch & CRM
    account_txn_count_30d           INT,
    account_txn_volume_30d          DECIMAL(18,2),
    crm_interaction_count_90d       INT,
    has_open_complaint              INT,
    
    -- Nhóm Marketing / Cross-sell Flags
    flag_cross_sell_credit_card     INT,
    flag_renew_deposit              INT,
    flag_upgrade_vip                INT,
    
    -- Mốc ngày chốt sổ
    cob_dt                          DATE
)
USING iceberg
PARTITIONED BY (days(cob_dt))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);