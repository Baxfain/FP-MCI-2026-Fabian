from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "mmds_engineer",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "marketplace_analytics_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    description="Marketplace Analytics Pipeline using Spark + ClickHouse",

) as dag:

    data_preparation = BashOperator(
        task_id="data_preparation",
        bash_command="python /opt/airflow/dags/scripts/data_preparation.py",
    )

    master_dataset = BashOperator(
        task_id="master_dataset",
        bash_command="python /opt/airflow/dags/scripts/master_dataset.py",
    )

    tfidf_analysis = BashOperator(
        task_id="tfidf_analysis",
        bash_command="python /opt/airflow/dags/scripts/tfidf.py",
    )

    process_spark = BashOperator(
        task_id="process_spark",
        bash_command="python /opt/airflow/dags/scripts/process_spark.py",
    )


    data_preparation >> master_dataset >> tfidf_analysis >> process_spark