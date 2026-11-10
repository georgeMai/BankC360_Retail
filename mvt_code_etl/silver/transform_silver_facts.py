import sys
import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SilverTransformFacts")

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("Silver_Transform_Facts")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://warehouse/silver")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "admin12345")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

def transform_facts(cob_dt: str):
    spark = create_spark_session()

    try:
        # ----------------------------------------------------------------------
        # 1. Transaction Fact: Giao dịch tài khoản (Deduplicate qua ROW_NUMBER)
        # ----------------------------------------------------------------------
        logger.info(f"--> Transform fact_account_txn cho COB_DT: {cob_dt}")
        spark.sql(f"""
            WITH deduplicated_txn AS (
                SELECT *,
                       ROW_NUMBER() OVER(PARTITION BY txn_id ORDER BY last_updated DESC) as rn
                FROM lakehouse.bronze.core_txn_account
                WHERE cob_dt = DATE '{cob_dt}'
            )
            INSERT OVERWRITE lakehouse.silver.fact_account_txn
            SELECT txn_id, account_id, customer_id, txn_date, txn_amount, txn_type,
                   debit_credit, balance_after, channel, description, counter_account,
                   created_ts, last_updated, cob_dt
            FROM deduplicated_txn
            WHERE rn = 1
        """)

        # ----------------------------------------------------------------------
        # 2. Transaction Fact: Giao dịch thẻ (Deduplicate qua ROW_NUMBER)
        # ----------------------------------------------------------------------
        logger.info(f"--> Transform fact_card_txn cho COB_DT: {cob_dt}")
        spark.sql(f"""
            WITH deduplicated_card_txn AS (
                SELECT *,
                       ROW_NUMBER() OVER(PARTITION BY txn_id ORDER BY last_updated DESC) as rn
                FROM lakehouse.bronze.card_txn
                WHERE cob_dt = DATE '{cob_dt}'
            )
            INSERT OVERWRITE lakehouse.silver.fact_card_txn
            SELECT txn_id, card_id, customer_id, txn_date, txn_amount, txn_type,
                   currency, merchant_name, merchant_category, channel, status,
                   created_ts, last_updated, cob_dt
            FROM deduplicated_card_txn
            WHERE rn = 1
        """)

        # ----------------------------------------------------------------------
        # 3. Event Fact: Tương tác CRM
        # ----------------------------------------------------------------------
        logger.info(f"--> Transform fact_crm_interaction cho COB_DT: {cob_dt}")
        spark.sql(f"""
            WITH deduplicated_crm AS (
                SELECT *,
                       ROW_NUMBER() OVER(PARTITION BY interaction_id ORDER BY last_updated DESC) as rn
                FROM lakehouse.bronze.crm_interaction
                WHERE cob_dt = DATE '{cob_dt}'
            )
            INSERT OVERWRITE lakehouse.silver.fact_crm_interaction
            SELECT interaction_id, customer_id, interaction_date, channel, direction,
                   subject, category, status, assigned_to, created_ts, last_updated, cob_dt
            FROM deduplicated_crm
            WHERE rn = 1
        """)

        # ----------------------------------------------------------------------
        # 4. Periodic Snapshot Fact: Sổ tiết kiệm theo ngày
        # ----------------------------------------------------------------------
        logger.info(f"--> Transform fact_deposit_snapshot cho COB_DT: {cob_dt}")
        spark.sql(f"""
            INSERT OVERWRITE lakehouse.silver.fact_deposit_snapshot
            SELECT deposit_id, account_id, customer_id, product_code, principal_amount,
                   interest_rate, term_months, open_date, maturity_date, status, last_updated, cob_dt
            FROM lakehouse.bronze.core_deposit
            WHERE cob_dt = DATE '{cob_dt}'
        """)

        # ----------------------------------------------------------------------
        # 5. Periodic Snapshot Fact: Khoản vay theo ngày
        # ----------------------------------------------------------------------
        logger.info(f"--> Transform fact_loan_snapshot cho COB_DT: {cob_dt}")
        spark.sql(f"""
            INSERT OVERWRITE lakehouse.silver.fact_loan_snapshot
            SELECT loan_id, customer_id, product_code, branch_code, loan_amount,
                   outstanding_balance, interest_rate, term_months, disbursement_date,
                   maturity_date, loan_status, last_updated, cob_dt
            FROM lakehouse.bronze.core_loan
            WHERE cob_dt = DATE '{cob_dt}'
        """)

        logger.info("=== BẮT ĐẦU VÀ HOÀN THÀNH TRANSFORM TẤT CẢ BẢNG FACT THÀNH CÔNG! ===")

    except Exception as e:
        logger.error(f"Lỗi khi Transform Silver Facts: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Vui lòng truyền cob_dt!")
        sys.exit(1)

    execution_date = sys.argv[1]
    transform_facts(execution_date)