.PHONY: up down reset

down:
	docker compose down

up:
	docker compose up -d --wait
	docker compose run --rm rpk cluster config set auto_create_topics_enabled false -X admin.hosts=redpanda:9644
	uv run --env-file .env scripts/init_floci.py
	uv run --env-file .env scripts/init_kafka.py
	docker compose ps

reset:
	docker compose down -v