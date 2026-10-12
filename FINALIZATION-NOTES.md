# FlowForge finalization notes

## Reliability and developer-experience changes

- Retry scheduling is persisted through the PostgreSQL outbox with an `availableAt` timestamp; Redis is not the retry source of truth.
- Retry state (`READY` and incremented `retryCount`) and the retry outbox row are written in one `JobFailureService` database transaction.
- Terminal failure state and the DLQ outbox row are persisted transactionally; Kafka publication is handled by the outbox publisher.
- The outbox publisher selects only due events and marks rows processed only after broker acknowledgement. Publication remains at-least-once; duplicate delivery is expected and guarded by job claiming.
- The claim service accepts only `READY` jobs whose event attempt matches the persisted retry count.
- Business-task failures are separated from persistence/coordinator failures. A database/coordinator error propagates to Kafka error handling rather than being incorrectly counted as a retryable task failure.
- Failure handling now distinguishes a scheduled retry, a terminal DLQ transition, and a stale/duplicate failure event so metrics are not inflated for ignored duplicates.
- Added unit tests for retryable failure, exhausted retries, ignored duplicate failure, and retry-attempt claim checks.
- Added an operations runbook and a GitHub Actions CI workflow.

## Verification status

The prior project notes reported a working app/API, a completed workflow execution, and 12 passing tests on the previous package. This environment has Java but does not have Maven or Docker installed, so this updated package could not be compiled, unit-tested, or integration-tested here. The newly added tests and code changes therefore remain **unverified until CI or a local `mvn clean verify` run passes**.

Before treating the package as verified, run:

```bash
mvn clean verify
docker compose config --quiet
docker compose up --build -d
docker compose ps
```

Then follow `OPERATIONS.md` for health checks and an end-to-end workflow run. This package is not being represented as certified production-ready; public deployment still requires security review, migrations, integration/failure-injection testing, and operational hardening.
