from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.extractors.data_extractor import DataExtractor
from src.utils.spark_utils import get_spark_session

default_args = {
    'owner': 'data-team',
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'csv_data_extraction',
    default_args=default_args,
    schedule_interval='0 */6 * * *',  # Every 6 hours
    catchup=False
)

def extract_csv_data(**context):
    spark = get_spark_session()
    extractor = DataExtractor(spark)
    
    # Configure source
    config = {
        'type': 'csv',
        'path': '/data/raw/daily_sales_{{ ds }}.csv'  # Date templated
    }
    
    df = extractor.extract_from_source(config)
    row_count = df.count()
    
    # Push metrics
    context['ti'].xcom_push(key='rows_extracted', value=row_count)
    
    # Save to staging
    df.write.mode('overwrite').parquet(f'/data/staging/sales_{{ ds }}')
    
    spark.stop()

extract_task = PythonOperator(
    task_id='extract_csv',
    python_callable=extract_csv_data,
    dag=dag
)