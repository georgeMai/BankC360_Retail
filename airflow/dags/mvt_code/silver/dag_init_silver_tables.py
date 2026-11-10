from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data_engineers",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="00_init_silver_tables_dag",
    default_args=default_args,
    description="Khởi tạo DDL toàn bộ bảng Silver (Dim từ file .sql, Fact từ .py)",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lakehouse", "silver", "ddl", "iceberg"],
) as dag:

    # --------------------------------------------------------------------------
    # TASK 1: Khởi tạo 5 bảng Dimension BẰNG FILE SQL
    # --------------------------------------------------------------------------
    create_dim_tables = BashOperator(
        task_id="create_silver_dim_tables_sql",
        bash_command="python /opt/airflow/jobs/run_sql.py /code_mvt/silver/dims_create.sql",
        execution_timeout=timedelta(minutes=10),
    )

    # --------------------------------------------------------------------------
    # TASK 2: Khởi tạo 5 bảng Fact (hoặc bạn đổi nốt file fact sang .sql tương tự)
    # --------------------------------------------------------------------------
    """ create_fact_tables = BashOperator(
        task_id="create_silver_fact_tables",
        bash_command="python /opt/airflow/jobs/create_silver_facts.py",
        execution_timeout=timedelta(minutes=10),
    ) """

    # Task 2: Tạo các bảng Fact từ file SQL
    create_fact_tables = BashOperator(
        task_id="create_silver_fact_tables_sql",
        bash_command="python /opt/airflow/jobs/run_sql.py /code_mvt/silver/facts_create.sql",
        execution_timeout=timedelta(minutes=10),
    )


    def notify_completion():
        print("==========================================================")
        print(" THÀNH CÔNG: Toàn bộ bảng Silver (Dim & Fact) đã khởi tạo!")
        print("==========================================================")

    finish_task = PythonOperator(
        task_id="init_completed_notification",
        python_callable=notify_completion,
    )

    [create_dim_tables, create_fact_tables] >> finish_task