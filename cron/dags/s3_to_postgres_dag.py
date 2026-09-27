""" 
This is a script to ingests data from an s3 bucket to a postgres database. The whole program was setup up on docker.
"""

from datetime import datetime, timedelta
from airflow.models import DAG
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


s3_hook = S3Hook(aws_conn_id="minio_conn")
pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
engine = pg_hook.get_sqlalchemy_engine()

bucket_name = "ecommercedataset"

source_prefix = 'data/'
archive_prefix = 'archive/'

target_schema = 'bronze'

    
default_args = {
    'owner' : "Leslie",
    'retries': 5,
    "retry_delay": timedelta(minutes=5)
}


def s3_to_postgres():
    # pulling data from s3 bucket
    files = s3_hook.list_keys(bucket_name=bucket_name, prefix=source_prefix)
    csv_files = [key for key in files if key.endswith('.csv')] if files else []
    
    if not csv_files:
        print(f"No files found to process under prefix: {source_prefix}")
        return
    print(f"found {len(csv_files)} files to parse.")
    
    #3.Process files one by one
    for file_key in csv_files:
        #extract filename (data/customers.csv - > customers.csv)
        filename_only = file_key.replace(source_prefix, '')
        
        target_table = filename_only.rsplit('.', 1)[0]
        
        print(f" --- Starting Processing: {file_key} -> Target Table: {target_schema}.{target_table}---")
        
        # download locally to handle chunking safely
        local_file_path = s3_hook.download_file(key=file_key, bucket_name=bucket_name)
        
        # Loop through chunks and append directly to Postgres
        chunk_size = 20000
        for chunk in pd.read_csv(local_file_path, chunksize=chunk_size):
            # chunk['source_file'] = filename_only
            # chunk['loaded_at'] = datetime.now()
            
            chunk = chunk.where(pd.notnull(chunk), None)
            # Append data to the pre-existing Bronze table
            chunk.to_sql(
                name=target_table,
                con=engine,
                schema=target_schema,
                if_exists='append',
                index=False
            )
        print(f"successfully loaded {file_key} into {target_schema}.{target_table}")
        
def archive_processed_files(ti):
    """Task 2: Pulls the list of successful filenames from XCom and moves them to S3 archive."""
    # Pull processed filenames from the first task
    files_to_archive = ti.xcom_pull(task_ids='parse_load_task')
    
    if not files_to_archive:
        print("No files were successfully processed. Skipping archiving.")
        return

    
    print(f"Received {len(files_to_archive)} files to archive: {files_to_archive}")

    for filename in files_to_archive:
        # Build relative paths clear of s3:// prefixes
        source_key = f"{source_prefix}{filename}"
        destination_key = f"{archive_prefix}{filename}"
        
        print(f"Moving S3 file: {source_key} -> {destination_key}")
        
        # 1. Copy to archive directory
        s3_hook.copy_object(
            source_bucket_name=bucket_name,
            source_bucket_key=source_key,
            dest_bucket_name=bucket_name,
            dest_bucket_key=destination_key
        )
        
        # 2. Delete original file
        s3_hook.delete_objects(bucket=bucket_name, keys=source_key)
        print(f"Successfully archived: {filename}")
            
    
with DAG(
    default_args=default_args,
    dag_id='s3_to_postgres_v009',
    start_date=datetime(2026,1,1),
    schedule='@daily'
) as dag:
    run_bronze_layer = SQLExecuteQueryOperator(
        task_id="run_bronze_layer",
        conn_id="postgres_conn",
        sql="CALL bronze_layer_creation();",
        autocommit=True
    )
    parse_load_task = PythonOperator(
        task_id="s3_to_postgres",
        python_callable=s3_to_postgres
    )
    
    archive_task = PythonOperator(
        task_id = 'archive_proceed_files_task',
        python_callable = archive_processed_files
    )


run_bronze_layer >> parse_load_task >> archive_task
