#ingesting data from source postgre & Oracle

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

# ==============================================================================
# CONFIGURATION MAP FOR ALL SOURCE TABLES
# ==============================================================================
INGESTION_CONFIG = {
    # --- ORACLE SOURCES (CORE BANKING) ---
    "branch": {
        "source_type": "oracle",
        "source_table": "CORE_BANKING.BRANCH",
        "target_table": "lakehouse.bronze.core_branch",
        "mode": "snapshot",
    },
    "product": {
        "source_type": "oracle",
        "source_table": "CORE_BANKING.PRODUCT",
        "target_table": "lakehouse.bronze.core_product",
        "mode": "snapshot",
    },
    "customer": {
        "source_type": "oracle",
        "source_table": "CORE_BANKING.CUSTOMER",
        "target_table": "lakehouse.bronze.core_customer",
        "mode": "snapshot",
    },
    "account": {
        "source_type": "oracle",
        "source_table": "CORE_BANKING.ACCOUNT",
        "target_table": "lakehouse.bronze.core_account",
        "mode": "snapshot",
    },
    "deposit": {
        "source_type": "oracle",
        "source_table": "CORE_BANKING.DEPOSIT",
        "target_table": "lakehouse.bronze.core_deposit",
        "mode": "snapshot",
    },
    "loan": {
        "source_type": "oracle",
        "source_table": "CORE_BANKING.LOAN",
        "target_table": "lakehouse.bronze.core_loan",
        "mode": "snapshot",
    },
    "txn_account": {
        "source_type": "oracle",
        "source_table": "CORE_BANKING.TXN_ACCOUNT",
        "target_table": "lakehouse.bronze.core_txn_account",
        "mode": "incremental",
        "date_col": "TXN_DATE",
    },
    # --- POSTGRESQL SOURCES (CARD & CRM) ---
    "card": {
        "source_type": "postgres",
        "source_table": "card_crm.card",
        "target_table": "lakehouse.bronze.card",
        "mode": "snapshot",
    },
    "card_txn": {
        "source_type": "postgres",
        "source_table": "card_crm.card_txn",
        "target_table": "lakehouse.bronze.card_txn",
        "mode": "incremental",
        "date_col": "txn_date",
    },
    "crm_interaction": {
        "source_type": "postgres",
        "source_table": "card_crm.crm_interaction",
        "target_table": "lakehouse.bronze.crm_interaction",
        "mode": "incremental",
        "date_col": "interaction_date",
    },
}

# JDBC Connection Settings
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


def create_spark_session() -> SparkSession:
    """Khởi tạo Spark Session cấu hình sẵn Apache Iceberg catalog"""
    return (
        SparkSession.builder.appName("Bronze_Data_Ingestion")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog"
        )
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://warehouse/bronze")
        # MinIO S3 configurations
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "admin12345")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def read_jdbc(
    spark: SparkSession, db_type: str, dbtable_or_query: str
):
    """Hàm helper đọc dữ liệu qua JDBC"""
    cfg = JDBC_CONFIGS[db_type]
    return (
        spark.read.format("jdbc")
        .option("url", cfg["url"])
        .option("driver", cfg["driver"])
        .option("user", cfg["user"])
        .option("password", cfg["password"])
        .option("dbtable", dbtable_or_query)
        .load()
    )


def ingest_table(spark: SparkSession, table_key: str, cob_dt: str):
    """Hàm chính xử lý ingest 1 bảng bất kỳ vào Bronze Iceberg"""
    if table_key not in INGESTION_CONFIG:
        raise ValueError(f"Không tìm thấy cấu hình cho bảng: {table_key}")

    cfg = INGESTION_CONFIG[table_key]
    db_type = cfg["source_type"]
    src_table = cfg["source_table"]
    target_table = cfg["target_table"]
    mode = cfg["mode"]

    print(f"[{table_key.upper()}] Bắt đầu ingest -> {target_table} (COB: {cob_dt})")

    # 1. Lập Query đọc dữ liệu nguồn
    if mode == "snapshot":
        # Full Snapshot: Đọc toàn bộ bảng ở thời điểm chạy
        query_sql = f"(SELECT * FROM {src_table}) src_tbl"
    else:
        # Incremental: Đọc các bản ghi phát sinh hoặc cập nhật trong ngày cob_dt
        date_col = cfg["date_col"]
        if db_type == "oracle":
            query_sql = f"""(
                SELECT * FROM {src_table}
                WHERE TRUNC({date_col}) = TO_DATE('{cob_dt}', 'YYYY-MM-DD')
                   OR TRUNC(last_updated) = TO_DATE('{cob_dt}', 'YYYY-MM-DD')
            ) src_tbl"""
        else:  # postgresql
            query_sql = f"""(
                SELECT * FROM {src_table}
                WHERE {date_col}::date = '{cob_dt}'::date
                   OR last_updated::date = '{cob_dt}'::date
            ) src_tbl"""

    df = read_jdbc(spark, db_type, query_sql)

    # 2. Chuẩn hóa Tên Cột (Chuyển sang lowercase) & Bổ sung cột cob_dt
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.lower())

    df = df.withColumn("cob_dt", F.to_date(F.lit(cob_dt)))

    # 3. Ghi vào Apache Iceberg Bằng Dynamic Overwrite theo Partition `cob_dt`
    # Xóa partition cob_dt cũ (nếu re-run) và overwrite đúng partition đó
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    df.writeTo(target_table).overwritePartitions()

    print(f"[{table_key.upper()}] Ingest thành công {df.count()} dòng vào partition cob_dt={cob_dt}!")


def main():
    if len(sys.argv) < 2:
        print("Cú pháp: python bronze_ingestion.py <cob_dt> [table_name]")
        print("Ví dụ: python bronze_ingestion.py 2026-07-21 customer")
        sys.exit(1)

    cob_dt = sys.argv[1]
    target_table_key = sys.argv[2] if len(sys.argv) > 2 else "ALL"

    spark = create_spark_session()

    if target_table_key.upper() == "ALL":
        for t_key in INGESTION_CONFIG.keys():
            ingest_table(spark, t_key, cob_dt)
    else:
        ingest_table(spark, target_table_key.lower(), cob_dt)

    spark.stop()


if __name__ == "__main__":
    main()