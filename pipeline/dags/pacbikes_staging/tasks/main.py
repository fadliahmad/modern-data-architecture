from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.datasets import Dataset

from pacbikes_staging.tasks.extract import Extract
from pacbikes_staging.tasks.load import Load

@task_group()
def extract(incremental: bool):
    """
    Task group to handle extraction of data from both database and API.
    
    Args:
        incremental (bool): Flag to determine if the extraction is incremental or full.
    """
    
    @task_group()
    def db():
        """
        Task group to handle extraction of data from the database.
        """
        table_to_extract = eval(Variable.get("PACBIKES_STAGING__table_to_extract_and_load"))
        
        for table_name, info in table_to_extract.items():
            schema = info[0]
            
            current_task = PythonOperator(
                task_id=f"{schema}.{table_name}",
                python_callable=Extract._db,
                op_kwargs={
                    'schema': schema,
                    'table_name': table_name,
                    'incremental': incremental
                },
                outlets=[Dataset(f"s3://pacbikes-db/{schema}/{table_name}/*.csv")]
            )
            
            current_task  # Register the task

    @task_group()
    def api():
        """
        Task group to handle extraction of data from the API.
        """
        url = Variable.get("PACBIKES_API_URL")
        
        current_task = PythonOperator(
            task_id="currency_data",
            python_callable=Extract._api,
            op_kwargs={
                'url': url
            },
            outlets=[Dataset("s3://pacbikes-api/data.csv")]
        )
        
        current_task  # Register the task

    if incremental:
        db()
    else:
        db()
        api()

@task_group()
def load(incremental: bool):
    """
    Task group to handle loading of data into the staging area.
    
    Args:
        incremental (bool): Flag to determine if the loading is incremental or full.
    """
    
    @task_group()
    def db():
        """
        Task group to handle loading of data from the database into the staging area.
        """
        table_to_load = eval(Variable.get("PACBIKES_STAGING__table_to_extract_and_load"))
        previous_task = None
        
        for table_name, info in table_to_load.items():
            schema = info[0]
            primary_key = info[1]
            
            current_task = PythonOperator(
                task_id=f"staging.{table_name}",
                python_callable=Load.load,
                op_kwargs={
                    'sources': 'db',
                    'schema': schema,
                    'table_name': table_name,
                    'primary_key': primary_key,
                    'incremental': incremental
                },
                outlets=[Dataset(f'postgres://warehouse:5432/postgres.pacbikes_staging.{table_name}')],
                trigger_rule='none_failed'
            )
            
            if previous_task:
                previous_task >> current_task  # Set task dependency
                
            previous_task = current_task  # Update previous task

    @task_group()
    def api():
        """
        Task group to handle loading of data from the API into the staging area.
        """
        current_task = PythonOperator(
            task_id="currency_data",
            python_callable=Load.load,
            op_kwargs={
                'sources': 'api',
                'schema': 'staging',
                'table_name': 'currency',
                'primary_key': 'currencycode',
                'incremental': incremental
            },
            outlets=[Dataset('postgres://warehouse:5432/postgres.pacbikes_staging.currency')],
            trigger_rule='none_failed'
        )
        
        current_task  # Register the task

    if incremental:
        db()
    else:
        db()
        api()