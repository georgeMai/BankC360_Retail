import os
import sys
import logging
from pyspark.sql import SparkSession

# Configuration Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("CreateBronzeTables")

# Đường dẫn mặc định tới file SQL DDL Bronze
DEFAULT_SQL_PATH = "/opt/airflow/code_mvt/ddl/create_bronze_tables.sql"

def create_spark_session() -> SparkSession:
    """Khởi tạo SparkSession cấu hình cho Apache Iceberg & MinIO Catalog"""
    return (
        SparkSession.builder.appName("Create_Bronze_Tables_From_SQL")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config(
            "spark.sql.catalog.lakehouse.warehouse", "s3a://warehouse/bronze"
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "admin12345")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )

def execute_sql_file(sql_file_path: str):
    """Đọc file .sql, bóc tách các câu lệnh DDL và thực thi lần lượt"""
    if not os.path.exists(sql_file_path):
        raise FileNotFoundError(f"Không tìm thấy file SQL tại đường dẫn: {sql_file_path}")

    logger.info(f"Đang đọc file SQL DDL từ: {sql_file_path}")
    with open(sql_file_path, "r", encoding="utf-8") as f:
        sql_content = f.read()

    # Tách các câu SQL phân cách bởi dấu ';'
    raw_statements = sql_content.split(";")
    statements = []

    for stmt in raw_statements:
        # Bỏ qua comment và các dòng trống
        lines = [line for line in stmt.split("\n") if not line.strip().startswith("--")]
        clean_stmt = "\n".join(lines).strip()
        if clean_stmt:
            statements.append(clean_stmt)

    logger.info(f"Tìm thấy {len(statements)} câu lệnh DDL cần thực thi.")

    spark = create_spark_session()

    try:
        for idx, stmt in enumerate(statements, 1):
            # In ra thông tin bảng đang xử lý
            first_line = stmt.split("\n")[0]
            logger.info(f"[{idx}/{len(statements)}] Executing: {first_line[:80]}...")
            spark.sql(stmt)

        logger.info("=== TẠO TOÀN BỘ BẢNG BRONZE TRÊN ICEBERG THÀNH CÔNG! ===")
    except Exception as e:
        logger.error(f"Lỗi khi thực thi SQL: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    # Cho phép truyền đường dẫn file SQL từ command line nếu cần
    sql_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_SQL_PATH
    execute_sql_file(sql_path)