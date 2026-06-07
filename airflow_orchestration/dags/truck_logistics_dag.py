from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'archit_sharma',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

PROJECT_DIR = "/opt/airflow/project"

with DAG(
    'truck_logistics_pipeline',
    default_args=default_args,
    description='Automated production gate for dbt transformations and ClickHouse serving.',
    schedule=None, # Trigger manually, or change to '@hourly'
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['logistics', 'production'],
) as dag:

    execute_dbt_run = BashOperator(
        task_id = 'dbt_transform_silver',
        bash_command = f'cd {PROJECT_DIR}/ai_logistics_transform && dbt run',
    )

    execute_dbt_test = BashOperator(
        task_id = 'dbt_data_quality_gates',
        bash_command = f'cd {PROJECT_DIR}/ai_logistics_transform && dbt test'
    )

    sync_to_clickhouse = BashOperator(
        task_id = 'sync_clickhouse_gold',
        bash_command = f'python {PROJECT_DIR}/gold_layer/gold_truck_ml_features.py'
    )

    execute_dbt_run >> execute_dbt_test >> sync_to_clickhouse