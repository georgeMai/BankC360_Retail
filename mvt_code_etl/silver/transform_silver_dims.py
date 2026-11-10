import sys
import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SilverTransformDims")

# Cấu hình danh sách 5 bảng Dim, Business Key và các Cột Tracked
DIM_CONFIGS = {
    "dim_customer": {
        "bronze_table": "lakehouse.bronze.core_customer",
        "silver_table": "lakehouse.silver.dim_customer",
        "business_key": ["customer_id"],
        "tracked_cols": ["phone", "email", "address", "city", "district", "customer_segment", "kyc_status", "branch_code"],
        "select_cols": "customer_id, cccd, full_name, gender, date_of_birth, phone, email, address, city, district, branch_code, customer_segment, kyc_status, register_date, is_active, last_updated"
    },
    "dim_account": {
        "bronze_table": "lakehouse.bronze.core_account",
        "silver_table": "lakehouse.silver.dim_account",
        "business_key": ["account_id"],
        "tracked_cols": ["product_code", "branch_code", "account_type", "currency", "balance", "status"],
        "select_cols": "account_id, account_no, customer_id, product_code, branch_code, account_type, currency, balance, open_date, close_date, status, last_updated"
    },
    "dim_product": {
        "bronze_table": "lakehouse.bronze.core_product",
        "silver_table": "lakehouse.silver.dim_product",
        "business_key": ["product_code"],
        "tracked_cols": ["product_name", "product_group", "product_type", "currency", "is_active"],
        "select_cols": "product_code, product_name, product_group, product_type, currency, is_active, launch_date, last_updated"
    },
    "dim_branch": {
        "bronze_table": "lakehouse.bronze.core_branch",
        "silver_table": "lakehouse.silver.dim_branch",
        "business_key": ["branch_code"],
        "tracked_cols": ["branch_name", "region", "city", "district", "address", "manager_name", "status"],
        "select_cols": "branch_code, branch_name, region, city, district, address, manager_name, open_date, status, last_updated"
    },
    "dim_card": {
        "bronze_table": "lakehouse.bronze.card",
        "silver_table": "lakehouse.silver.dim_card",
        "business_key": ["card_id"],
        "tracked_cols": ["account_id", "product_code", "card_type", "card_brand", "credit_limit", "status"],
        "select_cols": "card_id, card_no_masked, customer_id, account_id, product_code, card_type, card_brand, credit_limit, issue_date, expiry_date, status, last_updated"
    }
}

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("Silver_Transform_Dims_SCD2")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://warehouse/silver")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "admin12345")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )

def process_scd2(spark: SparkSession, dim_key: str, cob_dt: str):
    cfg = DIM_CONFIGS[dim_key]
    bronze_tbl = cfg["bronze_table"]
    silver_tbl = cfg["silver_table"]
    b_keys = cfg["business_key"]
    t_cols = cfg["tracked_cols"]
    select_cols = cfg["select_cols"]

    logger.info(f"==> Xử lý SCD2 cho bảng: [{silver_tbl}] với COB_DT: {cob_dt}")

    # 1. Đọc Snapshot ngày hôm nay từ Bronze
    source_df = spark.sql(f"""
        SELECT {select_cols}
        FROM {bronze_tbl}
        WHERE cob_dt = DATE '{cob_dt}'
    """)
    source_df.createOrReplaceTempView("stg_bronze_today")

    # 2. Xây dựng điều kiện JOIN & điều kiện thay đổi thông tin (Tracked Change Condition)
    join_cond = " AND ".join([f"target.{k} = source.{k}" for k in b_keys])
    change_cond = " OR ".join([f"target.{c} <=> source.{c} = FALSE" for c in t_cols])

    # 3. Đóng bản ghi cũ ở Silver (Set is_current = 0, effective_to = cob_dt - 1 day)
    expire_old_sql = f"""
    MERGE INTO {silver_tbl} AS target
    USING stg_bronze_today AS source
    ON {join_cond} AND target.is_current = 1
    WHEN MATCHED AND ({change_cond}) THEN
        UPDATE SET 
            target.is_current = 0,
            target.effective_to = DATE '{cob_dt}' - INTERVAL 1 DAY
    """
    spark.sql(expire_old_sql)

    # 4. Insert phiên bản mới (Bản ghi mới hoàn toàn HOẶC bản ghi vừa bị đóng phiên bản cũ)
    insert_new_sql = f"""
    INSERT INTO {silver_tbl}
    SELECT 
        source.*,
        DATE '{cob_dt}' AS effective_from,
        DATE '9999-12-31' AS effective_to,
        1 AS is_current
    FROM stg_bronze_today source
    LEFT JOIN (
        SELECT * FROM {silver_tbl} WHERE is_current = 1
    ) target
    ON {join_cond}
    WHERE target.{b_keys[0]} IS NULL OR ({change_cond})
    """
    spark.sql(insert_new_sql)
    logger.info(f"--> [THÀNH CÔNG] Hoàn tất SCD2 cho {silver_tbl}")

def main():
    if len(sys.argv) < 2:
        logger.error("Vui lòng truyền cob_dt!")
        sys.exit(1)

    cob_dt = sys.argv[1]
    spark = create_spark_session()

    try:
        for dim_key in DIM_CONFIGS.keys():
            process_scd2(spark, dim_key, cob_dt)
    except Exception as e:
        logger.error(f"Lỗi khi xử lý Silver Dims: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()