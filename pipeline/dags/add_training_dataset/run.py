from airflow.decorators import dag
from datetime import datetime
from airflow.operators.python import PythonOperator
from add_training_dataset.tasks.extract_load_valid_predictions import ExtractLoad

@dag(
    dag_id="add_training_dataset",
    start_date=datetime(2024, 9, 1),
    schedule="@daily",
    catchup=False,
    tags=["dellstore"],
    description="Extract, and Load Dellstore data into Staging Area"
)

def add_training_dataset():        
        
    extract_load_valid_predictions = PythonOperator(
        task_id = 'extract_load_valid_predictions',
        python_callable = ExtractLoad.prediction_data,
    )

    extract_load_valid_predictions

add_training_dataset()