# Spark Data Extractor on Kubernetes

Data extraction and transformation pipeline using Apache Airflow and PySpark on Kubernetes. The DAG launches ad‑hoc Spark Pods via `KubernetesPodOperator` (no Deployment/StatefulSet, no PVC/PV by default). Storage is expected to be external (object storage like MinIO/S3 via `s3a://`, or databases via JDBC).

## Features
- Data extraction from CSV/JSON/Parquet/JDBC (see `data-extract-usage-guide.md`).
- PySpark transform step run as an ad‑hoc Kubernetes Pod (ephemeral).
- Optional MinIO integration via `s3a://` (no PV/PVC required).
- Airflow-ready DAG: `dags/etl_pipeline.py` with `extract -> transform -> load` pattern.

## Repository Structure
```
├── dags/
│   ├── etl_pipeline.py                # Airflow DAG (uses KubernetesPodOperator for Spark)
│   └── spark_jobs/
│       └── transform_job.py           # Minimal PySpark transform job
├── src/
│   ├── extractors/                    # Data extraction helpers (skeleton)
│   ├── transformers/                  # Transformation helpers (skeleton)
│   ├── loaders/                       # Load helpers (skeleton)
│   └── utils/                         # Utilities and config helpers (skeleton)
├── kubernetes/                        # Example manifests (Airflow/Spark/Postgres, templates)
├── docker/                            # Dockerfiles (placeholders)
├── config/
│   ├── airflow.cfg                    # Airflow config (example placeholder)
│   └── pipeline_config.yaml           # Your pipeline settings (create/adjust)
├── tests/                             # Unit tests (skeletons)
├── requirements.txt                   # Python dependencies
└── data-extract-usage-guide.md        # Detailed usage/recipes for extraction
```

## Prerequisites
- Kubernetes cluster (v1.24+) and `kubectl` configured.
- A running Airflow (in-cluster recommended). The repo provides example manifests under `kubernetes/`, but they may need adaptation.
- Docker image for Spark that includes `spark-submit` and (if using MinIO/S3) Hadoop AWS dependencies.

## Setup
1) Install Python dependencies locally (if developing DAGs/jobs):
```bash
pip install -r requirements.txt
```

2) Deploy or connect Airflow to your cluster:
- If Airflow runs inside Kubernetes, the DAGs in `dags/` should be mounted/packaged so the scheduler/webserver can see them.
- Ensure the Airflow Kubernetes connection/in-cluster config allows creating Pods in your namespace.

3) Configure the DAG `dags/etl_pipeline.py`:
- The transform step uses `KubernetesPodOperator` to run `spark-submit` inside a container.
- By default, the template uses placeholder paths (`/path/to/raw` and `/path/to/curated`). Replace them with the storage you use.
- If using object storage (MinIO via `s3a://`), add the needed `--conf spark.hadoop.fs.s3a.*` flags to `spark-submit` and pass credentials via env/Secret. See the section below.

## Using MinIO (s3a) without PVC/PV
This repository is designed to work without persistent volumes. For external storage, prefer `s3a://` paths and pass Spark s3a configuration at submit time. Example `spark-submit` arguments inside the operator:
```bash
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
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.endpoint.region=us-east-1 \
  local:///opt/airflow/dags/spark_jobs/transform_job.py \
  --input s3a://raw-bucket/path/to/data \
  --output s3a://curated-bucket/path/to/output
```
Notes:
- Inject `MINIO_*` as environment variables to the Pod via `KubernetesPodOperator` (`env_vars` or `env_from` a Secret).
- Ensure your Spark image includes compatible `hadoop-aws` and AWS SDK jars for your Hadoop version.

## Airflow UI
Port-forward if you expose the webserver as a Service (adjust namespace/name as needed):
```bash
kubectl port-forward svc/airflow-webserver 8080:8080 -n data-pipeline
```
Then open http://localhost:8080

## Configuration and Usage
- See `data-extract-usage-guide.md` for detailed examples (CSV/JSON/Parquet/JDBC, incremental patterns, and DAG snippets).
- Put your pipeline parameters in `config/pipeline_config.yaml` (create if missing) and load them within your tasks/jobs as needed.

## Troubleshooting
- If Spark cannot access MinIO, verify the `fs.s3a` configuration and credentials, and that the Spark image has the required jars.
- If the Airflow task remains pending, check ServiceAccount/RBAC and the Airflow Kubernetes connection.

## License
MIT
