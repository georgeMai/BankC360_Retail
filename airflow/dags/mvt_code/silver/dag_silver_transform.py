from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data_engineers",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="dag_silver_transform",
    default_args=default_args,
    description="Pipeline Transform dữ liệu từ Bronze sang Silver Layer (Dimensions SCD2 & Facts)",
    schedule_interval=None,#"0 3 * * *",  # Chạy vào 03:00 AM hàng ngày
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["code_mvt", "silver", "transform", "scd2"],
) as dag:

    def start_silver_pipeline(**kwargs):
        print(f"=== BẮT ĐẦU TRANSFORM SILVER PIPELINE CHO COB_DT: {kwargs['ds']} ===")

    task_start = PythonOperator(
        task_id="start_silver_pipeline",
        python_callable=start_silver_pipeline,
        provide_context=True,
    )

    # Task 1: Transform Dimensions (SCD Type 2)
    task_transform_dims = BashOperator(
        task_id="transform_silver_dims_scd2",
        bash_command="python /opt/airflow/code_mvt/silver/transform_silver_dims.py {{ ds }}",
        execution_timeout=timedelta(minutes=30),
    )

    # Task 2: Transform Facts (Transaction & Snapshots)
    task_transform_facts = BashOperator(
        task_id="transform_silver_facts",
        bash_command="python /opt/airflow/code_mvt/silver/transform_silver_facts.py {{ ds }}",
        execution_timeout=timedelta(minutes=30),
    )

    def finish_silver_pipeline(**kwargs):
        print(f"=== KHỞI TẠO XONG SILVER PIPELINE CHO COB_DT: {kwargs['ds']} ===")

    task_finish = PythonOperator(
        task_id="finish_silver_pipeline",
        python_callable=finish_silver_pipeline,
        provide_context=True,
    )

    # Dims và Facts có thể chạy song song để tiết kiệm thời gian
    task_start >> [task_transform_dims, task_transform_facts] >> task_finish