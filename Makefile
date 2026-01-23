all: 
	docker compose up --build -d

stop: 
	docker compose down -v

logs-api:
	docker compose logs -f api