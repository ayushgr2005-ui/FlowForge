package com.flowforge.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.model.OutboxEvent;
import com.flowforge.repository.OutboxEventRepository;
import org.springframework.stereotype.Service;
import java.util.Map;
import java.time.LocalDateTime;

@Service
public class JobEventProducer {

    private final OutboxEventRepository outboxRepo;
    private final ObjectMapper objectMapper;

    public static final String MAIN_TOPIC = "workflow-job-execution";
    public static final String DLQ_TOPIC = "workflow-job-dlq";

    public JobEventProducer(OutboxEventRepository outboxRepo, ObjectMapper objectMapper) {
        this.outboxRepo = outboxRepo;
        this.objectMapper = objectMapper;
    }

    /**
     * Outbox Pattern: Saves to Postgres within DB Transaction.
     * Async OutboxPublisher drains it to Kafka without data loss.
     */
    public void dispatch(Long executionId, Long jobId, int retryAttempt, String inputContext) {
        enqueueOutbox(executionId, jobId, retryAttempt, inputContext);
    }
    public void enqueueOutbox(Long executionId, Long jobId, int retryAttempt, String inputContext) {
        enqueueOutboxAfterDelay(executionId, jobId, retryAttempt, inputContext, 0L);
    }

    public void enqueueOutboxAfterDelay(Long executionId, Long jobId, int retryAttempt,
                                        String inputContext, long delayMillis) {
        try {
            JobEventPayload payload = new JobEventPayload(executionId, jobId, retryAttempt, inputContext);
            String json = objectMapper.writeValueAsString(payload);
            LocalDateTime availableAt = LocalDateTime.now().plusNanos(Math.max(0L, delayMillis) * 1_000_000L);
            outboxRepo.save(new OutboxEvent("JOB_EXECUTION", executionId + ":" + jobId,
                    MAIN_TOPIC, json, availableAt));
        } catch (Exception e) {
            throw new RuntimeException("Failed to persist Outbox Event", e);
        }
    }

    public void enqueueDlqOutbox(Long executionId, Long jobId, String errorReason) {
        try {
            String dlqPayload = objectMapper.writeValueAsString(Map.of(
                    "executionId", executionId,
                    "jobId", jobId,
                    "reason", errorReason == null ? "Unknown failure" : errorReason));
            outboxRepo.save(new OutboxEvent("JOB_DEAD_LETTER", executionId + ":" + jobId,
                    DLQ_TOPIC, dlqPayload));
        } catch (Exception e) {
            throw new RuntimeException("Failed to persist DLQ outbox event", e);
        }
    }

    /** Kept for compatibility; application failure paths should use enqueueDlqOutbox. */
    public void sendToDLQ(Long executionId, Long jobId, String errorReason) {
        enqueueDlqOutbox(executionId, jobId, errorReason);
    }
}
