import sys
import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("GoldTransformCustomer360")

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("Gold_Mart_Customer_360_Transform")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://warehouse/gold")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "admin12345")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

def transform_gold(cob_dt: str):
    logger.info(f"=== BẮT ĐẦU TRANSFORM GOLD MART_CUSTOMER_360 CHO COB_DT: {cob_dt} ===")
    spark = create_spark_session()

    try:
        # Truy vấn tổng hợp dữ liệu từ lớp Silver
        gold_transform_query = f"""
        WITH 
        -- 1. Snapshot khách hàng active hiện tại từ Dim
        cust AS (
            SELECT * 
            FROM lakehouse.silver.dim_customer 
            WHERE is_current = 1
        ),

        -- 2. Aggregation Tài khoản thanh toán (CASA)
        acct_agg AS (
            SELECT 
                customer_id,
                SUM(CASE WHEN account_type = 'CASA' THEN balance ELSE 0 END) AS total_casa_balance,
                COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) AS count_active_accounts
            FROM lakehouse.silver.dim_account
            WHERE is_current = 1
            GROUP BY customer_id
        ),

        -- 3. Aggregation Tiết kiệm
        dep_agg AS (
            SELECT 
                customer_id,
                SUM(principal_amount) AS total_deposit_amount,
                COUNT(1) AS count_active_deposits,
                SUM(CASE WHEN maturity_date BETWEEN DATE '{cob_dt}' AND DATE '{cob_dt}' + INTERVAL 30 DAYS 
                         THEN principal_amount ELSE 0 END) AS deposit_maturing_next_30d_amount
            FROM lakehouse.silver.fact_deposit_snapshot
            WHERE cob_dt = DATE '{cob_dt}' AND status = 'ACTIVE'
            GROUP BY customer_id
        ),

        -- 4. Aggregation Khoản vay
        loan_agg AS (
            SELECT 
                customer_id,
                SUM(outstanding_balance) AS total_loan_outstanding,
                COUNT(CASE WHEN loan_status = 'ACTIVE' THEN 1 END) AS count_active_loans,
                MAX(CASE WHEN loan_status = 'OVERDUE' THEN 1 ELSE 0 END) AS has_overdue_loan,
                MAX(interest_rate) AS max_loan_interest_rate,
                SUM(CASE WHEN maturity_date BETWEEN DATE '{cob_dt}' AND DATE '{cob_dt}' + INTERVAL 60 DAYS 
                         THEN outstanding_balance ELSE 0 END) AS loan_maturing_next_60d_amount
            FROM lakehouse.silver.fact_loan_snapshot
            WHERE cob_dt = DATE '{cob_dt}'
            GROUP BY customer_id
        ),

        -- 5. Aggregation Thẻ
        card_agg AS (
            SELECT 
                customer_id,
                COUNT(CASE WHEN card_type = 'CREDIT' AND status = 'ACTIVE' THEN 1 END) AS count_active_credit_cards,
                COUNT(CASE WHEN card_type = 'DEBIT' AND status = 'ACTIVE' THEN 1 END) AS count_active_debit_cards,
                SUM(CASE WHEN card_type = 'CREDIT' AND status = 'ACTIVE' THEN credit_limit ELSE 0 END) AS total_credit_limit
            FROM lakehouse.silver.dim_card
            WHERE is_current = 1
            GROUP BY customer_id
        ),

        -- 6. Chi tiêu Thẻ 30 ngày qua
        card_txn_30d AS (
            SELECT 
                customer_id,
                SUM(txn_amount) AS card_spend_amount_30d
            FROM lakehouse.silver.fact_card_txn
            WHERE txn_date >= DATE '{cob_dt}' - INTERVAL 30 DAYS 
              AND txn_date <= DATE '{cob_dt}'
              AND status = 'SUCCESS'
            GROUP BY customer_id
        ),

        -- 7. Giao dịch Tài khoản 30 ngày qua
        acct_txn_30d AS (
            SELECT 
                customer_id,
                COUNT(1) AS account_txn_count_30d,
                SUM(txn_amount) AS account_txn_volume_30d
            FROM lakehouse.silver.fact_account_txn
            WHERE txn_date >= DATE '{cob_dt}' - INTERVAL 30 DAYS 
              AND txn_date <= DATE '{cob_dt}'
            GROUP BY customer_id
        ),

        -- 8. Tương tác CRM 90 ngày qua
        crm_agg AS (
            SELECT 
                customer_id,
                COUNT(1) AS crm_interaction_count_90d,
                MAX(CASE WHEN category = 'COMPLAINT' AND status = 'OPEN' THEN 1 ELSE 0 END) AS has_open_complaint
            FROM lakehouse.silver.fact_crm_interaction
            WHERE interaction_date >= DATE '{cob_dt}' - INTERVAL 90 DAYS 
              AND interaction_date <= DATE '{cob_dt}'
            GROUP BY customer_id
        )

        -- SELECT VÀ TÍNH TOÁN CÁC METRICS TỔNG HỢP + CROSS-SELL FLAGS
        SELECT 
            c.customer_id,
            
            -- Demographics
            CAST(YEAR(DATE '{cob_dt}') - YEAR(c.date_of_birth) AS INT) AS customer_age,
            CAST(DATEDIFF(MONTH, c.register_date, DATE '{cob_dt}') AS INT) AS tenure_months,
            c.customer_segment,
            c.kyc_status,
            
            -- CASA & Deposit
            CAST(COALESCE(a.total_casa_balance, 0) AS DECIMAL(18,2)) AS total_casa_balance,
            CAST(COALESCE(a.count_active_accounts, 0) AS INT) AS count_active_accounts,
            CAST(COALESCE(d.total_deposit_amount, 0) AS DECIMAL(18,2)) AS total_deposit_amount,
            CAST(COALESCE(d.count_active_deposits, 0) AS INT) AS count_active_deposits,
            CAST(COALESCE(d.deposit_maturing_next_30d_amount, 0) AS DECIMAL(18,2)) AS deposit_maturing_next_30d_amount,
            
            -- Loans
            CAST(COALESCE(l.total_loan_outstanding, 0) AS DECIMAL(18,2)) AS total_loan_outstanding,
            CAST(COALESCE(l.count_active_loans, 0) AS INT) AS count_active_loans,
            CAST(COALESCE(l.has_overdue_loan, 0) AS INT) AS has_overdue_loan,
            CAST(COALESCE(l.max_loan_interest_rate, 0) AS DECIMAL(5,2)) AS max_loan_interest_rate,
            CAST(COALESCE(l.loan_maturing_next_60d_amount, 0) AS DECIMAL(18,2)) AS loan_maturing_next_60d_amount,
            
            -- Cards
            CAST(COALESCE(cd.count_active_credit_cards, 0) AS INT) AS count_active_credit_cards,
            CAST(COALESCE(cd.count_active_debit_cards, 0) AS INT) AS count_active_debit_cards,
            CAST(COALESCE(cd.total_credit_limit, 0) AS DECIMAL(18,2)) AS total_credit_limit,
            CAST(COALESCE(ctx.card_spend_amount_30d, 0) AS DECIMAL(18,2)) AS card_spend_amount_30d,
            
            -- Behavior & CRM
            CAST(COALESCE(atx.account_txn_count_30d, 0) AS INT) AS account_txn_count_30d,
            CAST(COALESCE(atx.account_txn_volume_30d, 0) AS DECIMAL(18,2)) AS account_txn_volume_30d,
            CAST(COALESCE(crm.crm_interaction_count_90d, 0) AS INT) AS crm_interaction_count_90d,
            CAST(COALESCE(crm.has_open_complaint, 0) AS INT) AS has_open_complaint,
            
            -- Marketing Rules / Cross-sell Flags
            CASE 
                WHEN COALESCE(a.total_casa_balance, 0) >= 10000000 
                 AND COALESCE(cd.count_active_credit_cards, 0) = 0 
                 AND COALESCE(l.has_overdue_loan, 0) = 0 
                THEN 1 ELSE 0 
            END AS flag_cross_sell_credit_card,
            
            CASE 
                WHEN COALESCE(d.deposit_maturing_next_30d_amount, 0) > 0 
                THEN 1 ELSE 0 
            END AS flag_renew_deposit,
            
            CASE 
                WHEN c.customer_segment IN ('RETAIL', 'PRIORITY') 
                 AND (COALESCE(a.total_casa_balance, 0) + COALESCE(d.total_deposit_amount, 0)) >= 1000000000 
                THEN 1 ELSE 0 
            END AS flag_upgrade_vip,
            
            DATE '{cob_dt}' AS cob_dt

        FROM cust c
        LEFT JOIN acct_agg a      ON c.customer_id = a.customer_id
        LEFT JOIN dep_agg d       ON c.customer_id = d.customer_id
        LEFT JOIN loan_agg l      ON c.customer_id = l.customer_id
        LEFT JOIN card_agg cd     ON c.customer_id = cd.customer_id
        LEFT JOIN card_txn_30d ctx ON c.customer_id = ctx.customer_id
        LEFT JOIN acct_txn_30d atx ON c.customer_id = atx.customer_id
        LEFT JOIN crm_agg crm     ON c.customer_id = crm.customer_id
        """

        gold_df = spark.sql(gold_transform_query)
        cnt = gold_df.count()

        # Dynamic overwrite duy nhất partition cob_dt trên Iceberg
        target_table = "lakehouse.gold.mart_customer_360"
        gold_df.writeTo(target_table).overwritePartitions()

        logger.info(f"==> [THÀNH CÔNG] Đã tính toán và ghi {cnt} khách hàng vào bảng {target_table} cho cob_dt={cob_dt}")

    except Exception as e:
        logger.error(f"Lỗi khi Transform Gold: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Vui lòng truyền cob_dt! Ví dụ: python transform_gold_customer_360.py 2026-07-21")
        sys.exit(1)

    execution_date = sys.argv[1]
    transform_gold(execution_date)