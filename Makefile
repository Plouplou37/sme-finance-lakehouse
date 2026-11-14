# Every profile of docker-compose.yml: down and reset must stop and clean everything, not only the default
# services (a plain `docker compose down` leaves the pipeline, tools and jobs containers running).
ALL_PROFILES = --profile pipeline --profile tools --profile jobs

.PHONY: up pipeline down reset

# The lake only: Floci (S3) and Redpanda (Kafka), bucket and topics created. Enough for the generator, the
# producer, the Spark job or dbt run by hand.
up:
	docker compose up -d --wait
	docker compose run --rm rpk cluster config set auto_create_topics_enabled false -X admin.hosts=redpanda:9644
	uv run --env-file .env scripts/init_floci.py
	uv run --env-file .env scripts/init_kafka.py
	docker compose ps

# The lake, then the orchestration (M8): Airflow and the Spark Connect server. Kept apart from `up`: about
# 5 GB of memory limits more, on a WSL machine capped at 8 GB.
pipeline: up
	docker compose --profile pipeline up -d
	docker compose --profile pipeline ps

down:
	docker compose $(ALL_PROFILES) down

# Also deletes the volumes: S3 data, Kafka topics, Airflow history, dbt's DuckDB file in Airflow.
reset:
	docker compose $(ALL_PROFILES) down -v
