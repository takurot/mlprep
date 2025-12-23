from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'mlprep_user',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'mlprep_pipeline_demo',
    default_args=default_args,
    description='A simple DAG to run mlprep pipeline',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['mlprep', 'etl'],
) as dag:

    # Task to check/prepare data (placeholder)
    check_data = BashOperator(
        task_id='check_input_data',
        bash_command='[ -f /path/to/data/input.csv ] && echo "Data exists" || exit 1',
    )

    # Task to run mlprep
    # Assumes mlprep is in PATH or specify full path
    run_mlprep = BashOperator(
        task_id='run_mlprep_transform',
        bash_command='mlprep run /path/to/pipelines/pipeline.yaml',
    )

    check_data >> run_mlprep
