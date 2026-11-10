from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Default arguments cho DAG
default_args = {
    "owner": "data_engineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Khởi tạo DAG
with DAG(
    dag_id="00_dag_bronze_ingestion",
    default_args=default_args,
    description="Pipeline Ingestion dữ liệu từ Oracle & Postgres vào Bronze Layer (Apache Iceberg)",
    schedule_interval=None,#"0 2 * * *",  # Tự động chạy vào 02:00 AM mỗi ngày
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["code_mvt", "bronze", "ingestion", "lakehouse"],
) as dag:

    # Task 1: Thông báo bắt đầu tiến trình
    def start_pipeline(**kwargs):
        print(f"=== BẮT ĐẦU INGESTION CHO COB_DT: {kwargs['ds']} ===")

    task_start = PythonOperator(
        task_id="start_ingestion_pipeline",
        python_callable=start_pipeline,
        provide_context=True,
    )

    # Task 2: Chạy script bronze_ingestion.py
    # Variable {{ ds }} trong Airflow sẽ tự động lấy ngày chạy DAG (định dạng YYYY-MM-DD)
    # Tham số 'ALL' chỉ định ingest toàn bộ 10 bảng nguồn
    task_ingest_bronze = BashOperator(
        task_id="run_bronze_ingestion",
        bash_command="python /opt/airflow/code_mvt/bronze/bronze_ingestion.py {{ ds }} ALL",
        execution_timeout=timedelta(hours=1),
    )

    # Task 3: Thông báo hoàn tất tiến trình
    def end_pipeline(**kwargs):
        print(f"=== HOÀN THÀNH INGESTION BRONZE CHO COB_DT: {kwargs['ds']} ===")

    task_end = PythonOperator(
        task_id="end_ingestion_pipeline",
        python_callable=end_pipeline,
        provide_context=True,
    )

    # Thứ tự thực thi công việc
    task_start >> task_ingest_bronze >> task_end