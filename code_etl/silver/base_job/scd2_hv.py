import yaml
from jinja2 import Template
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def load_config(yaml_path: str, cob_dt: str) -> dict:
    """Đọc file YAML và render các biến Jinja2 (ví dụ: {{ cob_dt }})"""
    with open(yaml_path, "r", encoding="utf-8") as f:
        raw_yaml = f.read()

    # Render Jinja2 template cho đoạn SQL
    rendered_yaml = Template(raw_yaml).render(cob_dt=cob_dt)
    return yaml.safe_load(rendered_yaml)


def run_scd2_job(spark: SparkSession, yaml_path: str, cob_dt: str):
    # 1. Parse YAML Config
    config = load_config(yaml_path, cob_dt)

    target_table = f"{config['target']['catalog']}.{config['target']['schema']}.{config['target']['table']}"
    business_keys = config["business_key"]
    tracked_cols = config["tracked_columns"]

    eff_from_col = config["scd"]["effective_from_column"]
    eff_to_col = config["scd"]["effective_to_column"]
    curr_flag_col = config["scd"]["current_flag_column"]
    high_water_mark = config["scd"].get("high_water_mark", "9999-12-31")

    print(f"=== BẮT ĐẦU CHẠY SCD2 JOB CHO: {target_table} | COB: {cob_dt} ===")

    # 2. Đọc dữ liệu Bronze mới từ query SQL trong YAML
    source_df = spark.sql(config["sql"])

    # Tạo bảng tạm Staging cho dữ liệu nguồn ngày hôm nay
    source_df.createOrReplaceTempView("stg_source_today")

    # 3. Kiểm tra xem bảng Silver Target đã tồn tại chưa
    # Nếu chưa tồn tại (chạy lần đầu - Initial Load) -> Khởi tạo bảng
    if not spark.catalog.tableExists(target_table):
        print(
            f"Bảng {target_table} chưa tồn tại. Tiến hành khởi tạo (Initial Load)..."
        )
        init_df = (
            source_df.withColumn(eff_from_col, F.to_date(F.lit(cob_dt)))
            .withColumn(eff_to_col, F.to_date(F.lit(high_water_mark)))
            .withColumn(curr_flag_col, F.lit(1))
        )

        init_df.write.format("iceberg").mode("append").saveAsTable(target_table)
        print(f"Khởi tạo bảng {target_table} thành công!")
        return

    # 4. Thực hiện SCD2 MERGE INTO (Nếu bảng Silver đã tồn tại)
    # Điều kiện JOIN theo Business Key
    pk_join_cond = " AND ".join(
        [f"target.{k} = source.{k}" for k in business_keys]
    )

    # Điều kiện phát hiện thay đổi ở các Cột Tracked
    tracked_change_cond = " OR ".join(
        [f"target.{col} <=> source.{col} = FALSE" for col in tracked_cols]
    )

    # Đóng bản ghi cũ nếu có sự thay đổi thông tin tracked
    # Cập nhật is_current = 0, effective_to = cob_dt - 1
    close_old_records_sql = f"""
    MERGE INTO {target_table} AS target
    USING stg_source_today AS source
    ON {pk_join_cond} AND target.{curr_flag_col} = 1
    WHEN MATCHED AND ({tracked_change_cond}) THEN
        UPDATE SET 
            target.{curr_flag_col} = 0,
            target.{eff_to_col} = DATE '{cob_dt}' - INTERVAL 1 DAY
    """

    print("--> Đóng các bản ghi cũ (Expire old versions)...")
    spark.sql(close_old_records_sql)

    # Tìm các bản ghi mới HOẶC các bản ghi vừa bị đóng ở bước trên để INSERT phiên bản mới (is_current = 1)
    insert_new_records_sql = f"""
    INSERT INTO {target_table}
    SELECT 
        source.*,
        DATE '{cob_dt}' AS {eff_from_col},
        DATE '{high_water_mark}' AS {eff_to_col},
        1 AS {curr_flag_col}
    FROM stg_source_today source
    LEFT JOIN (
        SELECT * FROM {target_table} WHERE {curr_flag_col} = 1
    ) target
    ON {pk_join_cond}
    WHERE target.{business_keys[0]} IS NULL -- Bản ghi mới hoàn toàn
       OR ({tracked_change_cond})            -- Bản ghi vừa bị ngắt phiên bản ở bước trên
    """

    print("--> Chèn các bản ghi phiên bản mới (Insert new versions)...")
    spark.sql(insert_new_records_sql)

    print(f"=== HOÀN THÀNH SCD2 JOB CHO {target_table} ===")


# --- DEMO CÁCH SỬ DỤNG ---
if __name__ == "__main__":
    # Khởi tạo Spark Session cấu hình Iceberg & MinIO
    spark = (
        SparkSession.builder.appName("SCD2_YAML_Runner")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        # Thêm các cấu hình MinIO / S3 catalog nếu cần ở đây...
        .getOrCreate()
    )

    # Đường dẫn file YAML và Ngày chạy (thường truyền từ Airflow `{{ ds }}`)
    YAML_FILE_PATH = "dim_customer.yml"
    EXECUTION_DATE = "2026-07-21"

    # Chạy Job
    run_scd2_job(spark, yaml_path=YAML_FILE_PATH, cob_dt=EXECUTION_DATE)