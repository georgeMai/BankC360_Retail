import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("BronzeIngestion")

# ==============================================================================
# 1. CẤU HÌNH KẾT NỐI HỆ THỐNG NGUỒN (JDBC)
# ==============================================================================
JDBC_CONFIGS = {
    "oracle": {
        "url": "jdbc:oracle:thin:@//oracle-db:1521/XEPDB1",
        "driver": "oracle.jdbc.driver.OracleDriver",
        "user": "core_banking",
        "password": "CoreBanking123!",
    },
    "postgres": {
        "url": "jdbc:postgresql://postgres-db:5432/card_crm_db",
        "driver": "org.postgresql.Driver",
        "user": "postgres",
        "password": "PostgresPassword123!",
    },
}

# ==============================================================================
# 2. CẤU HÌNH MÁP BẢNG NGUỒN -> BẢNG BRONZE ICEBERG
# ==============================================================================
TABLE_MAP = {
    # --- ORACLE CORE BANKING (7 BẢNG) ---
    "core_branch": {
        "source_db": "oracle",
        "source_table": "CORE_BANKING.BRANCH",
        "target_table": "lakehouse.bronze.core_branch",
        "mode": "snapshot"
    },
    "core_product": {
        "source_db": "oracle",
        "source_table": "CORE_BANKING.PRODUCT",
        "target_table": "lakehouse.bronze.core_product",
        "mode": "snapshot"
    },
    "core_customer": {
        "source_db": "oracle",
        "source_table": "CORE_BANKING.CUSTOMER",
        "target_table": "lakehouse.bronze.core_customer",
        "mode": "snapshot"
    },
    "core_account": {
        "source_db": "oracle",
        "source_table": "CORE_BANKING.ACCOUNT",
        "target_table": "lakehouse.bronze.core_account",
        "mode": "snapshot"
    },
    "core_deposit": {
        "source_db": "oracle",
        "source_table": "CORE_BANKING.DEPOSIT",
        "target_table": "lakehouse.bronze.core_deposit",
        "mode": "snapshot"
    },
    "core_loan": {
        "source_db": "oracle",
        "source_table": "CORE_BANKING.LOAN",
        "target_table": "lakehouse.bronze.core_loan",
        "mode": "snapshot"
    },
    "core_txn_account": {
        "source_db": "oracle",
        "source_table": "CORE_BANKING.TXN_ACCOUNT",
        "target_table": "lakehouse.bronze.core_txn_account",
        "mode": "incremental",
        "date_col": "TXN_DATE"
    },

    # --- POSTGRESQL CARD & CRM (3 BẢNG) ---
    "card": {
        "source_db": "postgres",
        "source_table": "card_crm.card",
        "target_table": "lakehouse.bronze.card",
        "mode": "snapshot"
    },
    "card_txn": {
        "source_db": "postgres",
        "source_table": "card_crm.card_txn",
        "target_table": "lakehouse.bronze.card_txn",
        "mode": "incremental",
        "date_col": "txn_date"
    },
    "crm_interaction": {
        "source_db": "postgres",
        "source_table": "card_crm.crm_interaction",
        "target_table": "lakehouse.bronze.crm_interaction",
        "mode": "incremental",
        "date_col": "interaction_date"
    }
}

def create_spark_session() -> SparkSession:
    """Khởi tạo SparkSession tối ưu cho Apache Iceberg & MinIO"""
    return SparkSession.builder \
        .appName("Bronze_Ingestion_Engine") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "hadoop") \
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://warehouse/bronze") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "admin12345") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

def build_source_query(cfg: dict, cob_dt: str) -> str:
    """Xây dựng câu SQL đọc từ database nguồn theo cơ chế Snapshot / Incremental"""
    src_table = cfg["source_table"]
    db_type = cfg["source_db"]
    mode = cfg["mode"]

    if mode == "snapshot":
        return f"(SELECT * FROM {src_table}) src_tbl"
    else:
        date_col = cfg["date_col"]
        if db_type == "oracle":
            return f"""(
                SELECT * FROM {src_table}
                WHERE TRUNC({date_col}) = TO_DATE('{cob_dt}', 'YYYY-MM-DD')
                   OR TRUNC(last_updated) = TO_DATE('{cob_dt}', 'YYYY-MM-DD')
            ) src_tbl"""
        else: # postgresql
            return f"""(
                SELECT * FROM {src_table}
                WHERE {date_col}::date = '{cob_dt}'::date
                   OR last_updated::date = '{cob_dt}'::date
            ) src_tbl"""

def align_schema_with_target(spark: SparkSession, source_df, target_table_name: str, cob_dt: str):
    """
    Tự động gióng hàng (align) danh sách cột và kiểu dữ liệu của DataFrame nguồn 
    với Schema chính xác của bảng Iceberg đích trong Catalog.
    """
    target_schema = spark.table(target_table_name).schema
    source_cols_lower = {col.lower(): col for col in source_df.columns}

    select_exprs = []
    for field in target_schema:
        col_name = field.name
        col_type = field.dataType

        if col_name == "cob_dt":
            # Gán ngày COB
            select_exprs.append(F.to_date(F.lit(cob_dt)).cast(col_type).alias("cob_dt"))
        elif col_name in source_cols_lower:
            orig_col_name = source_cols_lower[col_name]
            select_exprs.append(F.col(orig_col_name).cast(col_type).alias(col_name))
        else:
            # Nếu cột trong Iceberg không có ở Nguồn -> Điền NULL
            select_exprs.append(F.lit(None).cast(col_type).alias(col_name))

    return source_df.select(*select_exprs)

def ingest_single_table(spark: SparkSession, table_key: str, cob_dt: str):
    """Xử lý Ingest cho 1 bảng cụ thể"""
    if table_key not in TABLE_MAP:
        raise ValueError(f"Không tìm thấy cấu hình cho bảng '{table_key}'!")

    cfg = TABLE_MAP[table_key]
    db_type = cfg["source_db"]
    target_table = cfg["target_table"]
    jdbc_cfg = JDBC_CONFIGS[db_type]

    logger.info(f"==> Bắt đầu Ingest: [{table_key}] -> Bảng đích: [{target_table}] | COB: {cob_dt}")

    # 1. Đọc dữ liệu từ DB Nguồn qua JDBC
    query_sql = build_source_query(cfg, cob_dt)
    raw_df = spark.read.format("jdbc") \
        .option("url", jdbc_cfg["url"]) \
        .option("driver", jdbc_cfg["driver"]) \
        .option("user", jdbc_cfg["user"]) \
        .option("password", jdbc_cfg["password"]) \
        .option("dbtable", query_sql) \
        .load()

    # 2. Khớp Schema với bảng Iceberg
    aligned_df = align_schema_with_target(spark, raw_df, target_table, cob_dt)

    # 3. Thực hiện Ghi đè Partition (Dynamic Overwrite) vào Iceberg
    record_count = aligned_df.count()
    if record_count > 0:
        aligned_df.writeTo(target_table).overwritePartitions()
        logger.info(f"--> [THÀNH CÔNG] Đã ghi {record_count} bản ghi vào partition cob_dt='{cob_dt}' của bảng {target_table}")
    else:
        logger.warning(f"--> [CẢNH BÁO] Không có bản ghi nào thu được từ nguồn cho ngày {cob_dt}.")

def main():
    if len(sys.argv) < 2:
        logger.error("Cú pháp không hợp lệ! Vui lòng truyền cob_dt.")
        logger.info("Ví dụ: python bronze_ingestion.py 2026-07-21 ALL")
        sys.exit(1)

    cob_dt = sys.argv[1]
    target_key = sys.argv[2] if len(sys.argv) > 2 else "ALL"

    spark = create_spark_session()

    try:
        if target_key.upper() == "ALL":
            for t_key in TABLE_MAP.keys():
                ingest_single_table(spark, t_key, cob_dt)
        else:
            ingest_single_table(spark, target_key.lower(), cob_dt)
    except Exception as e:
        logger.error(f"Lỗi trong quá trình Ingestion: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()