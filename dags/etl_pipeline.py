from __future__ import annotations

import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Optional: Uncomment if/when you are ready to run Spark jobs from Airflow
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# For ad-hoc Pods: we use the KubernetesPodOperator (no Deployment/StatefulSet created).
# It creates a single ephemeral Pod per task run, then the Pod completes and is cleaned up
# according to your Airflow/Kubernetes settings.
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import V1EnvFromSource, V1SecretEnvSource


logger = logging.getLogger(__name__)


def task_extract(**context):
    """Dummy extract task.
    Replace this with actual code to pull data from your sources (APIs, DB, files).
    """
    logger.info("[EXTRACT] Starting extraction...")
    # Example: read connection info from Airflow Connections or Variables
    # conn = BaseHook.get_connection("my_source")
    # result = fetch_data(conn)
    # context['ti'].xcom_push(key='raw_data_path', value='/path/to/raw/data')
    logger.info("[EXTRACT] Completed.")


def task_transform(**context):
    """Dummy transform task.
    Replace this with PySpark or pandas transformations.
    """
    logger.info("[TRANSFORM] Starting transformation...")
    # Example:
    # raw_data_path = context['ti'].xcom_pull(key='raw_data_path', task_ids='extract')
    # transformed_path = run_spark_job(input_path=raw_data_path, output_path='/path/to/curated')
    # context['ti'].xcom_push(key='curated_data_path', value=transformed_path)
    logger.info("[TRANSFORM] Completed.")


def task_load(**context):
    """Dummy load task.
    Replace this with code to load data into your target (e.g., Postgres, S3, Delta Lake).
    """
    logger.info("[LOAD] Starting load...")
    # Example:
    # curated_data_path = context['ti'].xcom_pull(key='curated_data_path', task_ids='transform')
    # load_to_postgres(curated_data_path)
    logger.info("[LOAD] Completed.")


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    description="Example ETL pipeline using Airflow",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["etl", "spark", "example"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=task_extract,
        provide_context=True,
    )

    # ----------------------------------------------------------------------------
    # KubernetesPodOperator as the ad-hoc Spark transform step
    # ----------------------------------------------------------------------------
    # This operator launches a single ephemeral Pod that runs spark-submit inside
    # the container image you provide. No Deployment/StatefulSet is created.
    #
    # Prerequisites:
    # - Airflow connection to the K8s cluster (Kubernetes cluster_conn_id) or
    #   in-cluster configuration when running Airflow inside K8s.
    # - The image must contain Spark binaries (spark-submit) and any dependencies.
    # - Appropriate ServiceAccount/RBAC if accessing cluster resources (e.g. driver->executors).
    # - Storage access (PVC/S3/GCS) if your job reads/writes data.
    spark_transform_pod = KubernetesPodOperator(
        task_id="spark_transform_pod",
        # If Airflow is running inside the same cluster, often in_cluster=True works.
        # Otherwise, set cluster_context/namespace via connection and remove in_cluster.
        namespace="data-pipeline",  # TODO: adjust to your namespace
        # in_cluster=True,  # Uncomment if Airflow runs inside K8s and you rely on in-cluster config

        # Image with Spark installed; choose a base you trust or your custom image.
        image="mon-registry-docker/spark-s3a:latest",  # Built from docker/Dockerfile.spark

        # Use /bin/sh -c to run a full spark-submit command with arguments.
        cmds=["/bin/sh", "-c"],
        arguments=[
            # spark-submit in cluster mode on Kubernetes with MinIO via s3a
            " \
            spark-submit \
              --master k8s://https://kubernetes.default.svc \
              --deploy-mode cluster \
              --name etl-transform \
              --conf spark.kubernetes.namespace=data-pipeline \
              --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
              --conf spark.executor.instances=2 \
              --conf spark.executor.memory=2g \
              --conf spark.driver.memory=1g \
              --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
              --conf spark.hadoop.fs.s3a.endpoint=$MINIO_ENDPOINT \
              --conf spark.hadoop.fs.s3a.access.key=$MINIO_ACCESS_KEY \
              --conf spark.hadoop.fs.s3a.secret.key=$MINIO_SECRET_KEY \
              --conf spark.hadoop.fs.s3a.path.style.access=true \
              --conf spark.hadoop.fs.s3a.connection.ssl.enabled=$MINIO_SSL_ENABLED \
              --conf spark.hadoop.fs.s3a.endpoint.region=$MINIO_REGION \
              local:///opt/airflow/dags/spark_jobs/transform_job.py \
              --input s3a://input/raw-bucket \
              --output s3a://output/curated-bucket"
        ],

        # Optionally stream logs from the Pod to Airflow task logs
        get_logs=True,

        # Service account with permissions for Spark-on-K8s (driver creates executor Pods)
        service_account_name="spark",  # TODO: ensure this exists in your cluster

        # Load environment variables from Kubernetes Secret (recommended)
        env_from=[
            V1EnvFromSource(secret_ref=V1SecretEnvSource(name="minio-credentials")),
        ],

        # Example of mounting volumes (PVCs) if you use persistent storage; adjust as needed
        # volumes=[...],
        # volume_mounts=[...],

        # Restart policy for the Pod
        is_delete_operator_pod=True,  # Clean up pod on success
    )

    load = PythonOperator(
        task_id="load",
        python_callable=task_load,
        provide_context=True,
    )

    # Example SparkSubmitOperator usage (uncomment and adjust if you have Spark cluster & app ready):
    # spark_transform = SparkSubmitOperator(
    #     task_id="spark_transform",
    #     application="/opt/airflow/dags/spark_jobs/transform_job.py",
    #     name="etl_spark_transform",
    #     conn_id="spark_default",  # Configure in Airflow
    #     application_args=["--input", "/path/to/raw", "--output", "/path/to/curated"],
    #     executor_memory="2g",
    #     driver_memory="1g",
    # )

    # Final dependency chain: extract -> spark_transform_pod -> load
    extract >> spark_transform_pod >> load

