from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from airflow import AirflowException
from helper.minio import CustomMinio
from sqlalchemy import create_engine

class ExtractLoad:
    @staticmethod
    def prediction_data(**kwargs):
        try:
            object_name = f'/predictions/data.csv'
            bucket_name = 'ml-bucket'
            
            # Create SQLAlchemy engine from Airflow connection
            engine = create_engine(PostgresHook(postgres_conn_id='credit-data-db-conn').get_uri())

            try:
                # Load DataFrame from MinIO
                df = CustomMinio._get_dataframe(bucket_name, object_name)
                print(df.head())
                # Ambil koneksi dan cursor
                pg_hook = PostgresHook(postgres_conn_id='credit-data-db-conn')
                conn = pg_hook.get_conn()
                cursor = conn.cursor()

                # Buat query insert sesuai jumlah kolom
                column_names = df.columns.tolist()
                column_str = ', '.join(column_names)
                placeholders = ', '.join(['%s'] * len(column_names))
                insert_query = f"INSERT INTO public.data_credit VALUES ({placeholders})"

                # Loop dan insert setiap baris
                for row in df.itertuples(index=False, name=None):
                    cursor.execute(insert_query, row)

                # Commit dan tutup koneksi
                conn.commit()
                cursor.close()
                conn.close()

            except Exception as e:
                engine.dispose()
                raise AirflowSkipException(f"Doesn't have data or insert failed: {str(e)}")

        except AirflowSkipException as e:
            raise e

        except Exception as e:
            raise AirflowException(f"Error when loading data")
