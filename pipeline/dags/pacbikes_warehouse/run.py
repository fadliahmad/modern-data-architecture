from airflow.decorators import dag, task
from airflow.models import Variable
from cosmos import DbtTaskGroup
from cosmos.config import ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles.postgres import PostgresUserPasswordProfileMapping
from cosmos.constants import TestBehavior
from pendulum import datetime
from helper.callbacks.slack_notifier import slack_notifier
import os


# For slack alerting
default_args = {
    'on_failure_callback': slack_notifier
}

# Define the path to the DBT project
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/pacbikes_warehouse/pacbikes_warehouse_dbt"

@dag(
    dag_id='pacbikes_warehouse',
    description='Transform data into warehouse',
    start_date=datetime(2024, 9, 1, tz="Asia/Jakarta"),
    schedule=None,
    catchup=False,
    default_args=default_args
)
def pacbikes_warehouse():
    """
    Main DAG function to orchestrate the pacbikes warehouse data transformation.
    """
    
    @task.branch
    def check_is_warehouse_init() -> str:
        """
        Task to check if the warehouse is initialized.
        
        Returns:
            str: The task ID to proceed with based on the warehouse initialization status.
        """
        PACBIKES_WAREHOUSE_INIT = Variable.get('PACBIKES_WAREHOUSE_INIT')
        PACBIKES_WAREHOUSE_INIT = eval(PACBIKES_WAREHOUSE_INIT)
        
        if PACBIKES_WAREHOUSE_INIT:
            return "warehouse_init"
        else:
            return "warehouse"
    
    # Task group for initializing the warehouse
    warehouse_init = DbtTaskGroup(
        group_id="warehouse_init",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
            project_name="pacbikes_warehouse"
        ),
        profile_config=ProfileConfig(
            profile_name="warehouse",
            target_name="warehouse",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id='warehouse',
                profile_args={"schema": "warehouse"}
            )
        ),
        render_config=RenderConfig(
            dbt_executable_path="/opt/airflow/dbt_venv/bin/dbt",
            emit_datasets=True,
            test_behavior=TestBehavior.AFTER_ALL
        ),
        operator_args={
            "install_deps": True,
            "full_refresh": True
        }
    )
    
    # Task group for regular warehouse operations
    warehouse = DbtTaskGroup(
        group_id="warehouse",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
            project_name="pacbikes_warehouse"
        ),
        profile_config=ProfileConfig(
            profile_name="warehouse",
            target_name="warehouse",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id='warehouse',
                profile_args={"schema": "warehouse"}
            )
        ),
        render_config=RenderConfig(
            dbt_executable_path="/opt/airflow/dbt_venv/bin/dbt",
            emit_datasets=True,
            test_behavior=TestBehavior.AFTER_ALL,
            exclude=["dim_date"]
        ),
        operator_args={
            "install_deps": True,
            "full_refresh": True
        }
    )
    
    # Define the task dependencies
    check_is_warehouse_init() >> [warehouse_init, warehouse]

# Instantiate the DAG
pacbikes_warehouse()