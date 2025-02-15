from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException, AirflowException
from helper.s3 import S3
from datetime import timedelta

import pandas as pd
import pytz
import requests

class Extract:
    """
    A class used to extract data from a database or an API and push it to S3.
    """

    @staticmethod
    def _db(schema: str, table_name: str, incremental: bool, **kwargs) -> None:
        """
        Extract data from a PostgreSQL database and push it to S3.

        Parameters:
        schema (str): The schema name.
        table_name (str): The table name.
        incremental (bool): Whether to extract data incrementally.
        kwargs: Additional keyword arguments.
        """
        try:
            # Get execution date and convert to Jakarta timezone
            ti = kwargs['ti']
            execution_date = ti.execution_date
            tz = pytz.timezone('Asia/Jakarta')
            execution_date = execution_date.astimezone(tz)
            data_date = (pd.to_datetime(execution_date) - timedelta(days=1)).strftime("%Y-%m-%d")
            
            # Connect to PostgreSQL database
            pg_hook = PostgresHook(postgres_conn_id='pacbikes-db')
            connection = pg_hook.get_conn()
            cursor = connection.cursor()

            # Formulate the extract query
            extract_query = f"SELECT * FROM {schema}.{table_name}"
            if incremental:
                extract_query += f" WHERE modifieddate::DATE = '{data_date}'::DATE;"
                object_name = f"pacbikes-db/{schema}/{table_name}/{data_date}.csv"
            else:
                object_name = f"pacbikes-db/{schema}/{table_name}/full_data.csv"
                
            # Execute the query and fetch results
            cursor.execute(extract_query)
            result = cursor.fetchall()
            column_list = [desc[0] for desc in cursor.description]
            cursor.close()
            connection.commit()
            connection.close()

            # Convert results to DataFrame
            df = pd.DataFrame(result, columns=column_list)

            # Check if DataFrame is empty and handle accordingly
            if df.empty:
                ti.xcom_push(
                    key=f"extract_info-{schema}.{table_name}", 
                    value={"status": "skipped", "data_date": data_date}
                )
                raise AirflowSkipException(f"Table '{schema}.{table_name}' doesn't have new data. Skipped...")
            else:
                ti.xcom_push(
                    key=f"extract_info-{schema}.{table_name}", 
                    value={"status": "success", "data_date": data_date}
                )
                
                # Push DataFrame to S3
                S3.push(
                    aws_conn_id='s3-conn',
                    bucket_name='pacbikes',
                    key=object_name,
                    string_data=df.to_csv(index=False)
                )
            
        except AirflowSkipException as e:
            raise e
        
        except AirflowException as e:
            raise AirflowException(f"Error when extracting {schema}.{table_name} : {str(e)}")

    @staticmethod
    def _api(url: str) -> None:
        """
        Extract data from an API and push it to S3.

        Parameters:
        url (str): The API endpoint URL.
        """
        # Fetch data from API and convert to DataFrame
        data_json = requests.get(url).json()
        df = pd.DataFrame(data_json)
        
        # Push DataFrame to S3
        S3.push(
            aws_conn_id='s3-conn',
            bucket_name='pacbikes',
            key='pacbikes-api/data.csv',
            string_data=df.to_csv(index=False)
        )