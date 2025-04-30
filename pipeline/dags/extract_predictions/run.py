from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from extract_predictions.tasks.extract_predictions import Extract

@dag(
    dag_id="extract_predictions",
    start_date=datetime(2024, 9, 1),
    schedule="@daily",
    catchup=False,
    tags=["dellstore"],
    description="Extract, and Load Dellstore data into Staging Area"
)

def extract_predictions():        
        
    extract_predictions = PythonOperator(
        task_id = 'extract_predictions',
        python_callable = Extract.extract_predictions,
    )

    extract_predictions

extract_predictions()