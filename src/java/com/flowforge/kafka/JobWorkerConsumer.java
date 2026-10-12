package com.flowforge.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.metrics.FlowForgeMetrics;
import com.flowforge.model.Enums.JobStatus;
import com.flowforge.model.Job;
import com.flowforge.repository.JobRepository;
import com.flowforge.service.ExecutionCoordinator;
import com.flowforge.service.JobExecutionClaimService;
import com.flowforge.service.JobFailureService;
import com.flowforge.service.JobFailureService.FailureOutcome;
import com.flowforge.service.RedisLockService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.UUID;

@Component
public class JobWorkerConsumer {
    private final JobRepository jobRepo;
    private final RedisLockService lockService;
    private final JobExecutionClaimService claimService;
    private final ExecutionCoordinator coordinator;
    private final JobFailureService failureService;
    private final FlowForgeMetrics metrics;
    private final ObjectMapper mapper;

    public JobWorkerConsumer(JobRepository jobRepo,
                             RedisLockService lockService,
                             JobExecutionClaimService claimService,
                             ExecutionCoordinator coordinator,
                             JobFailureService failureService,
                             FlowForgeMetrics metrics,
                             ObjectMapper mapper) {
        this.jobRepo = jobRepo;
        this.lockService = lockService;
        this.claimService = claimService;
        this.coordinator = coordinator;
        this.failureService = failureService;
        this.metrics = metrics;
        this.mapper = mapper;
    }

    @KafkaListener(topics = JobEventProducer.MAIN_TOPIC, groupId = "flowforge-workers")
    public void processTask(String message) {
        String token = UUID.randomUUID().toString();
        String lockKey = null;
        boolean locked = false;
        JobEventPayload event = null;

        try {
            event = mapper.readValue(message, JobEventPayload.class);
            Long executionId = event.executionId();
            Long jobId = event.jobId();
            lockKey = "lock:job:" + executionId + ":" + jobId;

            locked = lockService.acquireLock(lockKey, token, Duration.ofMinutes(2));
            if (!locked) {
                // Another worker owns this job. Kafka redelivery is safe due to DB claim.
                return;
            }

            // DB row lock + READY -> RUNNING transition is the authoritative idempotency guard.
            if (!claimService.claim(executionId, jobId, event.retryAttempt())) {
                return;
            }

            Job job = jobRepo.findById(jobId).orElseThrow();

            long start = System.currentTimeMillis();
            String result;
            try {
                // Only business-task failures consume retry attempts. Persistence or
                // coordinator failures must escape to Kafka error handling instead.
                result = executeTaskLogic(job, event.inputContext());
            } catch (Exception taskEx) {
                metrics.recordDuration(System.currentTimeMillis() - start);
                metrics.incrementFailed();
                String reason = taskEx.getMessage() == null
                        ? taskEx.getClass().getSimpleName() : taskEx.getMessage();
                FailureOutcome outcome = failureService.recordFailure(
                        executionId, jobId, event.inputContext(), reason);
                if (outcome == FailureOutcome.DEAD_LETTERED) {
                    metrics.incrementDLQ();
                }
                return;
            }

            metrics.recordDuration(System.currentTimeMillis() - start);
            // If this DB state transition fails, the outer handler throws so Kafka
            // can redeliver. It must not be misclassified as a business-task failure.
            coordinator.onJobSuccess(executionId, jobId, result);
            metrics.incrementCompleted();
        } catch (Exception e) {
            // Do not acknowledge a logical success when state transition or persistence failed.
            System.err.println("Worker processing error: " + e.getMessage());
            throw new IllegalStateException("Worker processing failed; allow Kafka retry", e);
        } finally {
            if (locked && lockKey != null) {
                try {
                    lockService.releaseLock(lockKey, token);
                } catch (Exception releaseError) {
                    System.err.println("Could not release worker lock: " + releaseError.getMessage());
                }
            }
        }
    }

    private String executeTaskLogic(Job job, String context) throws Exception {
        String safeContext = context == null ? "" : context;
        return switch (job.getType()) {
            case LOG -> "LOGGED(" + job.getName() + ")";
            case COMPUTE -> {
                Thread.sleep(1200);
                yield "COMPUTE_SHA256(" + safeContext.hashCode() + ")";
            }
            case TRANSFORM -> "TRANSFORMED(" + safeContext.toLowerCase() + ")";
            case HTTP_DISPATCH -> {
                if (Math.random() < 0.25) {
                    throw new RuntimeException("504 Gateway Timeout");
                }
                yield "HTTP_200_DISPATCHED";
            }
        };
    }
}
