from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime
from pacbikes_staging.tasks.main import extract, load
from helper.callbacks.slack_notifier import slack_notifier

# For slack alerting
default_args = {
    'on_failure_callback': slack_notifier
}

# Define the DAG with its properties
@dag(
    dag_id='pacbikes_staging',
    description='Extract data and load into staging area',
    start_date=datetime(2024, 9, 1, tz="Asia/Jakarta"),
    schedule="@daily",
    catchup=False,
    default_args=default_args
)
def pacbikes_staging():
    """
    DAG function to extract data and load it into the staging area.
    It also triggers the next DAG for loading data into the warehouse.
    """
    # Get the incremental mode from Airflow Variables
    incremental_mode = Variable.get('PACBIKES_STAGING_INCREMENTAL_MODE')
    incremental_mode = eval(incremental_mode)  # Convert string to boolean

    # Define the task to trigger the next DAG
    trigger_pacbikes_warehouse = TriggerDagRunOperator(
        task_id='trigger_pacbikes_warehouse',
        trigger_dag_id="pacbikes_warehouse",
        trigger_rule="none_failed"
    )

    # Define the task dependencies
    extract(incremental=incremental_mode) >> load(incremental=incremental_mode) >> trigger_pacbikes_warehouse

# Instantiate the DAG
pacbikes_staging()