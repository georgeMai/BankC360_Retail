import sys
from pyspark.sql import SparkSession


def run_sql_file(sql_file_path: str):
    print(f"=== KHỞI TẠO SPARK SESSION ĐỂ CHẠY FILE: {sql_file_path} ===")

    spark = (
        SparkSession.builder.appName(f"Run_SQL_{sql_file_path}")
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
            "spark.sql.catalog.lakehouse.warehouse", "s3a://warehouse/silver"
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "admin12345")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )

    # Đọc nội dung file SQL
    with open(sql_file_path, "r", encoding="utf-8") as f:
        sql_content = f.read()

    # Tách các câu lệnh theo dấu chấm phẩy ';'
    statements = [stmt.strip() for stmt in sql_content.split(";") if stmt.strip()]

    print(f"--> Tìm thấy {len(statements)} câu lệnh SQL cần thực thi.")

    for idx, stmt in enumerate(statements, 1):
        # Bỏ qua các dòng comment đơn thuần
        lines = [line for line in stmt.split("\n") if not line.strip().startswith("--")]
        clean_stmt = "\n".join(lines).strip()

        if not clean_stmt:
            continue

        print(f"[{idx}/{len(statements)}] Đang thực thi statement...")
        print(f"{clean_stmt[:100]}...")  # In 100 ký tự đầu tiên
        spark.sql(clean_stmt)

    print(f"=== THỰC THI THÀNH CÔNG FILE SQL: {sql_file_path} ===")
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Cú pháp: python run_sql.py <path_to_sql_file>")
        sys.exit(1)

    sql_path = sys.argv[1]
    run_sql_file(sql_path)