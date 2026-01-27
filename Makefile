all: 
	docker compose up --build -d api mysql mlflow grafana prometheus node-exporter airflow airflow-scheduler

stop: 
	docker compose down -v

logs-api:
	docker compose logs -f api