from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'data-team',
    'start_date': datetime(2025, 1, 1),
    'retries': 3
}

dag = DAG(
    'database_incremental_extraction',
    default_args=default_args,
    schedule_interval='0 1 * * *',  # Daily at 1 AM
    catchup=False
)

def extract_incremental_data(**context):
    from src.extractors.data_extractor import DataExtractor
    from src.utils.spark_utils import get_spark_session
    
    spark = get_spark_session()
    extractor = DataExtractor(spark)
    
    # Get last execution date
    execution_date = context['ds']
    
    # Incremental extraction query
    query = f"""
    (SELECT * FROM orders 
     WHERE updated_at >= '{execution_date}' 
     AND updated_at < '{execution_date}'::date + interval '1 day'
    ) AS orders_incremental
    """
    
    config = {
        'type': 'jdbc',
        'jdbc_url': os.getenv('DB_JDBC_URL'),
        'table': query,
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'driver': 'org.postgresql.Driver'
    }
    
    df = extractor.extract_from_source(config)
    
    # Save partitioned by date
    df.write.mode('append').partitionBy('updated_at') \
      .parquet('/data/warehouse/orders')
    
    spark.stop()

extract_task = PythonOperator(
    task_id='extract_incremental',
    python_callable=extract_incremental_data,
    dag=dag
)