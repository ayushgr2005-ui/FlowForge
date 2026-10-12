package com.flowforge.service;

import com.flowforge.kafka.JobEventProducer;
import com.flowforge.model.Enums.JobStatus;
import com.flowforge.model.JobExecution;
import com.flowforge.repository.JobExecutionRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/** Persists retry/DLQ state and its outbox event atomically in PostgreSQL. */
@Service
public class JobFailureService {
    public enum FailureOutcome { RETRY_SCHEDULED, DEAD_LETTERED, IGNORED }
    private final JobExecutionRepository jobExecutionRepo;
    private final JobEventProducer producer;
    private final ExecutionCoordinator coordinator;

    public JobFailureService(JobExecutionRepository jobExecutionRepo, JobEventProducer producer,
                             ExecutionCoordinator coordinator) {
        this.jobExecutionRepo = jobExecutionRepo;
        this.producer = producer;
        this.coordinator = coordinator;
    }

    /** Records a retry or terminal failure; duplicate/stale failures are ignored. */
    @Transactional
    public FailureOutcome recordFailure(Long executionId, Long jobId, String inputContext, String reason) {
        JobExecution record = jobExecutionRepo.findForUpdateByExecutionIdAndJobId(executionId, jobId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "No job execution for executionId=" + executionId + ", jobId=" + jobId));
        if (record.getStatus() != JobStatus.RUNNING) {
            return FailureOutcome.IGNORED;
        }

        int nextAttempt = record.getRetryCount() + 1;
        if (nextAttempt <= record.getMaxRetries()) {
            long delayMillis = (1L << Math.min(nextAttempt, 10)) * 1000L;
            record.setRetryCount(nextAttempt);
            record.setStatus(JobStatus.READY);
            record.setLastHeartbeat(LocalDateTime.now());
            jobExecutionRepo.save(record);
            producer.enqueueOutboxAfterDelay(executionId, jobId, nextAttempt, inputContext, delayMillis);
            return FailureOutcome.RETRY_SCHEDULED;
        }

        record.setRetryCount(nextAttempt);
        record.setStatus(JobStatus.DEAD_LETTER);
        record.setCompletedAt(LocalDateTime.now());
        jobExecutionRepo.save(record);
        producer.enqueueDlqOutbox(executionId, jobId, reason);
        coordinator.onJobTerminalFailure(executionId, jobId);
        return FailureOutcome.DEAD_LETTERED;
    }
}
