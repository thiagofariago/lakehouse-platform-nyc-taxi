.PHONY: help setup up down restart logs clean clean-all validate

# Default target
.DEFAULT_GOAL := help

# Load environment variables (if exists)
-include .env
export

##@ General

help: ## Display this help message
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Setup

setup: ## Initial setup - copy .env file
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "✅ Created .env file from .env.example"; \
		echo "⚠️  Please review and adjust values in .env if needed"; \
	else \
		echo "⚠️  .env file already exists, skipping"; \
	fi

validate: ## Validate environment and prerequisites
	@echo "🔍 Validating environment..."
	@command -v docker >/dev/null 2>&1 || { echo "❌ Docker is not installed"; exit 1; }
	@docker compose version >/dev/null 2>&1 || { echo "❌ Docker Compose is not installed"; exit 1; }
	@[ -f .env ] || { echo "❌ .env file not found. Run 'make setup' first"; exit 1; }
	@echo "✅ Environment validated"

##@ Docker Operations

up: validate ## Start all services
	@echo "🚀 Starting Distributed Lakehouse..."
	docker compose up -d
	@echo ""
	@echo "✅ Services started successfully!"
	@echo ""
	@echo "📊 Access points:"
	@echo "  - MinIO Console:    http://localhost:$(MINIO_CONSOLE_PORT) ($(MINIO_ROOT_USER)/$(MINIO_ROOT_PASSWORD))"
	@echo "  - Trino UI:         http://localhost:$(TRINO_COORDINATOR_PORT)"
	@echo "  - Airflow UI:       http://localhost:$(AIRFLOW_WEBSERVER_PORT) (admin/admin)"
	@echo ""
	@echo "⏳ Wait 30-60 seconds for all services to initialize"
	@echo "📝 Check logs: make logs"

up-build: validate ## Start all services with rebuild
	@echo "🔨 Building and starting services..."
	docker compose up -d --build

down: ## Stop all services
	@echo "🛑 Stopping all services..."
	docker compose down
	@echo "✅ Services stopped"

restart: ## Restart all services
	@echo "🔄 Restarting services..."
	docker compose restart
	@echo "✅ Services restarted"

scale-trino-workers: ## Scale Trino workers (usage: make scale-trino-workers WORKERS=3)
	@echo "⚙️  Scaling Trino workers to $(WORKERS)..."
	docker compose up -d --scale trino-worker=$(WORKERS)
	@echo "✅ Trino workers scaled to $(WORKERS)"

scale-spark-workers: ## Scale Spark workers (usage: make scale-spark-workers WORKERS=3)
	@echo "⚙️  Scaling Spark workers to $(WORKERS)..."
	docker compose up -d --scale spark-worker=$(WORKERS)
	@echo "✅ Spark workers scaled to $(WORKERS)"

##@ Logs

logs: ## Show logs from all services
	docker compose logs -f

logs-airflow: ## Show Airflow logs
	docker compose logs -f airflow-webserver airflow-scheduler

logs-trino: ## Show Trino logs
	docker compose logs -f trino-coordinator trino-worker

logs-spark: ## Show Spark logs
	docker compose logs -f spark-master spark-worker

logs-metastore: ## Show Hive Metastore logs
	docker compose logs -f hive-metastore

logs-minio: ## Show MinIO logs
	docker compose logs -f minio

##@ Airflow

airflow-trigger: ## Trigger the NYC Taxi pipeline DAG
	@echo "▶️  Triggering NYC Taxi pipeline..."
	docker compose exec airflow-scheduler airflow dags trigger nyc_taxi_pipeline
	@echo "✅ Pipeline triggered. Monitor at http://localhost:$(AIRFLOW_WEBSERVER_PORT)"

airflow-list-dags: ## List all Airflow DAGs
	docker compose exec airflow-scheduler airflow dags list

airflow-shell: ## Open Airflow scheduler shell
	docker compose exec airflow-scheduler /bin/bash

##@ dbt

dbt-debug: ## Run dbt debug
	docker compose exec airflow-scheduler dbt debug --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt

dbt-run: ## Run dbt models
	docker compose exec airflow-scheduler dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt

dbt-test: ## Run dbt tests
	docker compose exec airflow-scheduler dbt test --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt

dbt-docs: ## Generate and serve dbt documentation
	docker compose exec airflow-scheduler dbt docs generate --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt

##@ Spark

spark-shell: ## Open Spark master shell
	docker compose exec spark-master /bin/bash

spark-submit-test: ## Test Spark submit with a simple job
	@echo "🧪 Testing Spark submit..."
	docker compose exec spark-master spark-submit --version

##@ Trino

trino-cli: ## Open Trino CLI
	docker compose exec trino-coordinator trino --catalog iceberg --schema staging

trino-ui: ## Open Trino UI in browser
	@echo "Opening Trino UI..."
	@python3 -m webbrowser "http://localhost:$(TRINO_COORDINATOR_PORT)" 2>/dev/null || \
		echo "📊 Open manually: http://localhost:$(TRINO_COORDINATOR_PORT)"

##@ Monitoring

status: ## Show status of all services
	@echo "📊 Service Status:"
	@docker compose ps

health: ## Check health of all services
	@echo "🏥 Health Check:"
	@docker compose ps --format "table {{.Name}}\t{{.Status}}"

minio-ui: ## Open MinIO Console in browser
	@echo "Opening MinIO Console..."
	@python3 -m webbrowser "http://localhost:$(MINIO_CONSOLE_PORT)" 2>/dev/null || \
		echo "📦 Open manually: http://localhost:$(MINIO_CONSOLE_PORT)"

airflow-ui: ## Open Airflow UI in browser
	@echo "Opening Airflow UI..."
	@python3 -m webbrowser "http://localhost:$(AIRFLOW_WEBSERVER_PORT)" 2>/dev/null || \
		echo "🌬️  Open manually: http://localhost:$(AIRFLOW_WEBSERVER_PORT)"

##@ Cleanup

clean: ## Remove containers and networks (keeps volumes)
	@echo "🧹 Cleaning up containers and networks..."
	docker compose down
	@echo "✅ Cleanup complete (volumes preserved)"

clean-all: ## Remove everything including volumes (WARNING: deletes all data!)
	@echo "⚠️  WARNING: This will delete ALL data including:"
	@echo "  - MinIO data (raw data and lakehouse)"
	@echo "  - Metastore database"
	@echo "  - Airflow database"
	@echo ""
	@read -p "Are you sure? Type 'yes' to continue: " confirm; \
	if [ "$$confirm" = "yes" ]; then \
		echo "🧹 Removing everything..."; \
		docker compose down -v; \
		rm -rf airflow/logs/*; \
		echo "✅ Complete cleanup done"; \
	else \
		echo "❌ Cleanup cancelled"; \
	fi

clean-logs: ## Clean Airflow logs
	@echo "🧹 Cleaning Airflow logs..."
	rm -rf airflow/logs/*
	@echo "✅ Logs cleaned"

##@ Troubleshooting

debug-metastore: ## Debug Hive Metastore connection
	@echo "🔍 Testing Metastore connection..."
	docker compose exec postgres-metastore psql -U $(POSTGRES_METASTORE_USER) -d $(POSTGRES_METASTORE_DB) -c "\dt"

debug-minio: ## Check MinIO buckets
	@echo "🔍 Checking MinIO buckets..."
	docker compose exec minio mc ls local/

debug-trino-catalogs: ## Show Trino catalogs
	@echo "🔍 Showing Trino catalogs..."
	docker compose exec trino-coordinator trino --execute "SHOW CATALOGS;"

debug-trino-schemas: ## Show Trino schemas in Iceberg catalog
	@echo "🔍 Showing Trino schemas..."
	docker compose exec trino-coordinator trino --catalog iceberg --execute "SHOW SCHEMAS;"

debug-spark: ## Check Spark master status
	@echo "🔍 Checking Spark status..."
	docker compose exec spark-master spark-submit --version

troubleshoot: ## Run all troubleshooting checks
	@echo "🔧 Running comprehensive troubleshooting..."
	@echo ""
	@$(MAKE) status
	@echo ""
	@$(MAKE) debug-minio
	@echo ""
	@$(MAKE) debug-metastore
	@echo ""
	@$(MAKE) debug-trino-catalogs
	@echo ""
	@$(MAKE) debug-spark
