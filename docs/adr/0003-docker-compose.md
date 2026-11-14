# ADR 0003: Run the whole stack with Docker Compose on one machine

- **Status:** Accepted (M1, extended in M4 and M8)
- **Date:** 2026-11-08, written down on 2026-11-14

## Context

The platform needs about ten services: the AWS emulator (Floci and its Athena engine floci-duck), a Kafka broker (Redpanda), Spark, a Spark Connect server, Airflow (api-server, scheduler, dag-processor, a one-shot init) and its PostgreSQL database, plus two command-line clients (AWS CLI, rpk).

Constraints:
- **One laptop:** Windows 10, Docker Desktop on WSL 2, with the WSL virtual machine capped at 8 GB for every container together.
- **Public repository:** anyone must be able to clone it and run it with Docker and uv only, without installing Kafka, Spark or Airflow.
- **Reproducible:** the same versions on every machine, today and in six months.
- **No privileged access:** no container should be able to control the host.

## Decision

1. **One `docker-compose.yml`**, project name `sme-lake`, no `container_name`: services find each other by their service name on the project network (`floci:4566`, `redpanda:9092`, `spark-connect:15002`).
2. **Profiles, so only what is needed runs:**

   | Profile | Services | Started by |
   |---|---|---|
   | *(default)* | floci, floci-duck, redpanda | `make up` |
   | `tools` | awscli, rpk | `docker compose run --rm …` |
   | `jobs` | spark (one-off job) | `docker compose run --rm spark` |
   | `pipeline` | Airflow, PostgreSQL, spark-connect | `make pipeline` |

3. **Every image pinned by tag and digest** (`VERSIONS.md`), every Spark jar checked against its Maven Central SHA-1 at build time.
4. **A memory cap on every service** (`mem_limit`): 1 GiB for Floci and Redpanda, 512 MiB for floci-duck, 1.5 GiB for Spark and the Airflow scheduler, 256 MiB for PostgreSQL.
5. **Health checks decide the start order:** `docker compose up --wait` returns only when the services are healthy, and the Airflow scheduler waits for a healthy api-server (`depends_on: condition: service_healthy`).
6. **No Docker socket in any container.** Mounting `/var/run/docker.sock` gives root on the host. Floci therefore starts no containers (Redpanda and floci-duck are our own services), and Airflow sends work over the network instead of starting containers (ADR 0008).
7. **State in named volumes** (S3 data, Kafka log, Airflow database, DuckDB warehouse, task logs): `make down` keeps them, `make reset` (`down -v`) deletes them.
8. **Fake credentials in `.env`**, copied from `.env.example`. Compose refuses to start if one is missing (`${VAR:?…}`), instead of starting with an empty value.
9. **A `Makefile` with four commands** (`up`, `pipeline`, `down`, `reset`). Each recipe line is a single command, so it runs the same in `cmd.exe` on Windows and in `sh` on Linux.

## What was proven

| Check | Result |
|---|---|
| Start from nothing | ✅ `make reset` then `make up`: volumes created, bucket and prefixes created, Kafka topics created |
| Run twice | ✅ a second `make up` changes nothing and reports `Bucket already exists` |
| Another machine | ✅ the same `make up` on a second Windows machine, from a fresh clone |
| Start order | ✅ the scheduler once started a run before the api-server was ready, and the task was killed (M8). Since the health-check dependency, `make pipeline` shows the scheduler waiting until the api-server is `Healthy`, then starting |
| No socket | ✅ checked with `docker inspect`; Floci logs which services would need it, and S3, Glue and Athena are not among them (M0) |

## Limits

- **One machine, no high availability:** if the laptop stops, everything stops. Compose restarts a container, it does not move it to another machine.
- **The memory of the whole stack was never measured** at peak; the caps are estimates that held for this volume. `make pipeline` asks Docker for about 5 GB more.
- **Tested on Windows only.** The CI runs on Linux but without Docker (ADR 0009), so the Compose file itself is not tested there.
- **Start-up is slow the first time:** the Airflow image takes several minutes to build, and Airflow answers about a minute after the start.

## When not to use it

- **Production with several machines,** automatic restarts on another host, scaling with the load, rolling upgrades: Kubernetes, or managed services (Amazon S3, MSK, EMR or Glue jobs, MWAA).
- **A single small job:** a script and DuckDB do not need ten containers.

## Alternatives considered

| Alternative | Why not |
|---|---|
| Install the services on the host | Not reproducible, different on every operating system, and hard to remove |
| Kubernetes on the laptop (kind, minikube) | More memory and many more concepts, for no benefit on one machine |
| Let Floci start its own containers through the Docker socket | Root-equivalent access, no memory limit on the containers it starts, and its Kafka broker did not survive a restart (ADR 0001) |
