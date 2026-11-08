.PHONY: up down reset

down:
	docker compose down

up:
	docker compose up -d --wait
	uv run --env-file .env scripts/init_floci.py
	docker compose ps

reset:
	docker compose down -v