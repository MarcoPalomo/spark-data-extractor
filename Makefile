# Project Makefile

# ---------- Config ----------
REG ?= http://localhost:5000/v2/
NAMESPACE ?= data-pipeline
SPARK_IMG ?= $(REG)/spark-s3a:latest
AIRFLOW_IMG ?= $(REG)/airflow-pipeline:latest

# ---------- Docker ----------
.PHONY: build-spark push-spark build-airflow push-airflow

build-spark:
	docker build -t $(SPARK_IMG) -f docker/Dockerfile.spark .

push-spark:
	docker push $(SPARK_IMG)

build-airflow:
	docker build -t $(AIRFLOW_IMG) -f docker/Dockerfile.airflow .

push-airflow:
	docker push $(AIRFLOW_IMG)

# ---------- Kubernetes ----------
.PHONY: k8s-apply k8s-delete

k8s-apply:
	kubectl apply -f kubernetes/namespace.yaml
	kubectl apply -f kubernetes/postgresql/postgres-secret.yaml -n $(NAMESPACE)
	kubectl apply -f kubernetes/postgresql/postgres-init-sql.yaml -n $(NAMESPACE)
	kubectl apply -f kubernetes/postgresql/postgres-deployment.yaml -n $(NAMESPACE)
	kubectl apply -f kubernetes/postgresql/postgres-service.yaml -n $(NAMESPACE)
	kubectl apply -f kubernetes/secrets.yaml -n $(NAMESPACE)
	kubectl apply -f kubernetes/spark/spark-serviceaccount.yaml -n $(NAMESPACE)
	# Ensure Airflow DB is initialized before starting webserver/scheduler
	- kubectl delete job airflow-db-init -n $(NAMESPACE) --ignore-not-found
	kubectl apply -f kubernetes/airflow/airflow-db-init-job.yaml -n $(NAMESPACE)
	kubectl wait --for=condition=complete --timeout=180s job/airflow-db-init -n $(NAMESPACE)
	kubectl apply -f kubernetes/airflow/airflow-scheduler-deployment.yaml -n $(NAMESPACE)
	kubectl apply -f kubernetes/airflow/airflow-deployment.yaml -n $(NAMESPACE)
	kubectl apply -f kubernetes/airflow/airflow-webserver-service.yaml -n $(NAMESPACE)

k8s-delete:
	- kubectl delete -f kubernetes/airflow/airflow-webserver-service.yaml -n $(NAMESPACE) --ignore-not-found
	- kubectl delete -f kubernetes/airflow/airflow-deployment.yaml -n $(NAMESPACE) --ignore-not-found
	- kubectl delete -f kubernetes/airflow/airflow-scheduler-deployment.yaml -n $(NAMESPACE) --ignore-not-found
	- kubectl delete -f kubernetes/spark/spark-serviceaccount.yaml -n $(NAMESPACE) --ignore-not-found
	- kubectl delete -f kubernetes/secrets.yaml -n $(NAMESPACE) --ignore-not-found
	- kubectl delete -f kubernetes/postgresql/postgres-service.yaml -n $(NAMESPACE) --ignore-not-found
	- kubectl delete -f kubernetes/postgresql/postgres-deployment.yaml -n $(NAMESPACE) --ignore-not-found
	- kubectl delete -f kubernetes/postgresql/postgres-init-sql.yaml -n $(NAMESPACE) --ignore-not-found
	- kubectl delete -f kubernetes/postgresql/postgres-secret.yaml -n $(NAMESPACE) --ignore-not-found
	- kubectl delete -f kubernetes/namespace.yaml --ignore-not-found

# ---------- Tests ----------
.PHONY: test

test:
	pytest -q
