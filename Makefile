.PHONY: run stop build logs test simulate psql redis-cli clean

run:
	docker compose up --build

stop:
	docker compose down

build:
	docker compose build --no-cache

logs:
	docker compose logs -f app

test:
	pytest tests/ -v

simulate:
	python simulator/simulate.py --count 1000 --concurrency 100

psql:
	docker compose exec postgres psql -U payuser -d payments

redis-cli:
	docker compose exec redis redis-cli

clean:
	docker compose down -v
