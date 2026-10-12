# FlowForge operations and verification runbook

This runbook is for local development and portfolio demonstrations. It is not a production deployment certification.

## 1. Prerequisites

- Docker Desktop with Docker Compose v2
- Java 17 and Maven 3.9+ if running tests outside Docker
- Ports 8081, 5432, 6379 and 9092 available

## 2. Configure and start

```bash
cp .env.example .env
# Change POSTGRES_PASSWORD in .env before sharing the machine or exposing any port.
docker compose config
docker compose up --build -d
docker compose ps
```

Wait until Postgres, Redis, Kafka and the app are healthy/running. Then check:

```bash
curl -fsS http://localhost:8081/actuator/health
curl -fsS http://localhost:8081/api/workflows
curl -fsS http://localhost:8081/actuator/prometheus
```

The app is mapped to host port **8081** by Compose. The in-container application port is **8080**.

## 3. Automated tests

Run before building the Docker image:

```bash
mvn test
```

The unit suite covers DAG validation, atomic job claiming, stale retry-attempt rejection, retry-state persistence decisions, and terminal DLQ decisions. These unit tests use mocks; they do not replace an integration test against real Postgres/Kafka/Redis.

## 4. End-to-end smoke test

1. `GET /api/workflows`; note the sample workflow ID returned by the API.
2. `POST /api/executions/trigger/{workflowId}` for that ID.
3. Read `executionId` from the response.
4. Poll `GET /api/executions/{executionId}/status` until all jobs are terminal (`SUCCESS` or `DEAD_LETTER`).
5. Check `GET /actuator/health` and `/actuator/prometheus`.
6. Inspect logs with `docker compose logs --tail=200 app kafka postgres`.

`HTTP_DISPATCH` is deliberately simulated and randomly fails to exercise retry/DLQ behavior. It does not make a real outbound HTTP request.

## 5. Troubleshooting

```bash
docker compose ps
docker compose logs --tail=200 app
docker compose logs --tail=200 kafka
docker compose restart app
```

If startup fails because the database is unavailable, wait for the Postgres health check and inspect its logs. If a topic is not present, Kafka auto-topic creation is enabled for this local one-broker environment.

To stop without deleting persisted data:

```bash
docker compose down
```

To delete all local database and Redis data (destructive):

```bash
docker compose down -v
```

## 6. Delivery semantics and known limits

- Workflow state changes and event intent are written to PostgreSQL together using the outbox pattern.
- The publisher marks an outbox row processed only after a Kafka acknowledgement. A crash after broker acknowledgement but before the database commit can publish the same event again; consumers must remain idempotent.
- Job claiming uses a database row lock and checks both `READY` state and the expected retry attempt.
- Retry delay is persisted as `availableAt` in the outbox; Redis is used for token-based locks, not as the retry source of truth.
- The current Compose topology is a local demo topology (single Kafka broker, plaintext Kafka, development credentials). Do not expose it to the public internet.
- Before production use, add migrations, authentication/authorization, request DTO validation, structured logging, distributed tracing, Testcontainers integration tests, backup/restore drills, and multi-broker operational policies.
