from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from pangres import upsert
from sqlalchemy import create_engine
from helper.s3 import S3


class Load:
    """
    Class to handle loading data from S3 to a PostgreSQL database.
    """
    
    @staticmethod
    def load(sources: str, schema: str, table_name: str, primary_key: str, incremental: bool, **kwargs) -> None:
        """
        Load data from S3 to PostgreSQL database.

        :param sources: Source type ('db' or other).
        :param schema: Database schema name.
        :param table_name: Table name in the database.
        :param primary_key: Primary key of the table.
        :param incremental: Boolean flag for incremental load.
        :param kwargs: Additional keyword arguments.
        """
        ti = kwargs['ti']
        
        # Handle data extraction from database source
        if sources == 'db':
            extract_info = ti.xcom_pull(key=f"extract_info-{schema}.{table_name}")
            
            # Skip if no new data
            if extract_info["status"] == "skipped":
                raise AirflowSkipException(f"There is no new data for '{schema}.{table_name}'. Skipped...")
            
            # Determine S3 key based on incremental flag
            if incremental:
                key = f"pacbikes-db/{schema}/{table_name}/{extract_info['data_date']}.csv"
            else:
                key = f"pacbikes-db/{schema}/{table_name}/full_data.csv"
            
            # Pull data from S3
            df = S3.pull(aws_conn_id='s3-conn', bucket_name='pacbikes', key=key)
        
        # Handle data extraction from other sources
        else:
            df = S3.pull(aws_conn_id='s3-conn', bucket_name='pacbikes', key=f"pacbikes-api/data.csv")
        
        # Set primary key as index
        df = df.set_index(primary_key)
        
        # Create PostgreSQL engine
        postgres_uri = PostgresHook(postgres_conn_id='warehouse').get_uri()
        engine = create_engine(postgres_uri)
        
        # Upsert data into PostgreSQL table
        upsert(con=engine, df=df, table_name=table_name, schema='pacbikes_staging', if_row_exists='update')
        
        # Dispose the engine
        engine.dispose()