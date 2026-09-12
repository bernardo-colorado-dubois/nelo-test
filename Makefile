SHELL := /bin/bash
# PYTHONPATH=. -> los scripts viven en spark/, pero import src.* espera la
# raíz del proyecto en sys.path (python solo agrega el directorio del
# script, no el cwd). Mismo mecanismo que PYTHONPATH=/opt dentro de Docker.
VENV_PY := PYTHONPATH=. venv/bin/python
COMPOSE := docker compose

.PHONY: pipeline read transform export check-creds install clean \
        stack-build stack-init stack-up stack-down stack-restart stack-ps \
        stack-logs stack-clean stack-reset stack-flower stack-airflow-shell \
        stack-spark-shell stack-trigger

# =============================================================================
# Pipeline local (venv + Spark local[*], sin Docker) — ver CLAUDE.md
# =============================================================================
pipeline: read transform export

read:
	$(VENV_PY) spark/read_queue.py

transform:
	$(VENV_PY) spark/transform_messages.py

export:
	$(VENV_PY) spark/export_csv.py

check-creds:
	set -a && source .env && set +a && aws sts get-caller-identity

# El constraint fija boto3/python-dotenv a las versiones que Airflow 3.3.1
# espera (ver requirements.txt y CLAUDE.md, "Entorno local") — sin esto,
# pip no logra resolver apache-airflow junto con el resto del archivo.
AIRFLOW_CONSTRAINTS := https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt

install:
	python3 -m venv venv
	$(VENV_PY) -m pip install --upgrade pip -q
	$(VENV_PY) -m pip install -r requirements.txt --constraint "$(AIRFLOW_CONSTRAINTS)" -q
	@# apache-airflow-providers-apache-spark instala pyspark-client, que pisa
	@# archivos del paquete pyspark real bajo el mismo namespace -> reinstalar
	@# pyspark limpio encima para que import pyspark siga siendo el nuestro.
	$(VENV_PY) -m pip uninstall -y pyspark pyspark-client -q
	$(VENV_PY) -m pip install pyspark==3.5.3 -q

clean:
	rm -rf data output/items_flat.csv __pycache__ src/__pycache__

# =============================================================================
# Stack Airflow + Spark (docker-compose.yaml) — mismo ETL, orquestado.
# =============================================================================
stack-build: ## Solo construye la imagen custom de Airflow
	$(COMPOSE) build

stack-init: ## Primera vez: crea dirs, fija AIRFLOW_UID, migra DB, crea usuario admin
	@mkdir -p ./dags ./logs ./plugins ./config ./data ./output
	@if ! grep -q "^AIRFLOW_UID=" .env 2>/dev/null; then \
		echo "AIRFLOW_UID=$$(id -u)" >> .env; \
	fi
	@chmod -R 777 ./data ./output
	$(COMPOSE) up airflow-init

stack-up: ## Levanta todo el stack (Airflow + Spark) en background
	@mkdir -p ./data ./output
	@chmod -R 777 ./data ./output 2>/dev/null || true
	$(COMPOSE) up -d --build

stack-down: ## Detiene y quita todos los contenedores
	$(COMPOSE) down

stack-restart: stack-down stack-up ## Reinicia todo

stack-ps: ## Estado/salud de todos los servicios
	$(COMPOSE) ps

stack-logs: ## Sigue los logs de todo el stack (Ctrl+C para salir)
	$(COMPOSE) logs -f

stack-clean: ## Detiene contenedores y borra volúmenes (BORRA la metadata DB de Postgres)
	$(COMPOSE) down -v --remove-orphans

stack-reset: stack-clean stack-init stack-up ## Reset total: borra todo y arranca de cero

stack-flower: ## Levanta Flower (monitor de Celery) en :5555
	$(COMPOSE) --profile flower up -d flower

stack-airflow-shell: ## Shell dentro del contenedor del API server de Airflow
	$(COMPOSE) exec airflow-apiserver bash

stack-spark-shell: ## Spark shell interactivo contra el cluster
	$(COMPOSE) exec spark-master /opt/spark/bin/spark-shell --master spark://spark-master:7077

stack-trigger: ## Dispara el DAG etl_pipeline manualmente
	$(COMPOSE) exec airflow-apiserver airflow dags trigger etl_pipeline
