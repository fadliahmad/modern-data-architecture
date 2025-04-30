from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from airflow import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from helper.minio import CustomMinio
from datetime import timedelta
from airflow.models import Variable

import pandas as pd

BASE_PATH = "/opt/airflow/dags"


class Extract:
    @staticmethod
    def extract_predictions(**kwargs):
        try:
            pg_hook = PostgresHook(postgres_conn_id='credit-data-db-conn')
            connection = pg_hook.get_conn()
            cursor = connection.cursor()

            date = kwargs['ds']
            query = f"SELECT * FROM loan_predictions;"
            object_name = f'/predictions/data.csv'
    
            cursor.execute(query)
            result = cursor.fetchall()
            connection.commit()
            connection.close()

            column_list = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(result, columns=column_list)
            cursor.close()
        
            if df.empty:
                raise AirflowSkipException(f"Predictions table doesn't have new data. Skipped...")

            bucket_name = 'ml-bucket'
            CustomMinio._put_csv(df, bucket_name, object_name)

        except AirflowSkipException as e:
            raise e
        except Exception as e:
            raise AirflowException(f"Error when extract data : {str(e)}")