all: 
	docker compose up --build -d 

stop: 
	docker compose down 

logs-api:
	docker compose logs -f api

logs-airflow:
	docker compose logs -f airflow