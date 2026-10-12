package com.flowforge.service;

import com.flowforge.model.Enums.JobStatus;
import com.flowforge.model.JobExecution;
import com.flowforge.repository.JobExecutionRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * Atomically claims a READY job so duplicate Kafka deliveries cannot start
 * the same execution concurrently.
 */
@Service
public class JobExecutionClaimService {
    private final JobExecutionRepository repository;

    public JobExecutionClaimService(JobExecutionRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public boolean claim(Long executionId, Long jobId, int retryAttempt) {
        JobExecution record = repository.findForUpdateByExecutionIdAndJobId(executionId, jobId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "No job execution for executionId=" + executionId + ", jobId=" + jobId));
        // Outbox retries persist retryCount and READY in the same DB transaction.
        // Only the matching attempt may claim the job; stale/duplicate messages are ignored.
        if (record.getStatus() != JobStatus.READY || retryAttempt != record.getRetryCount()) {
            return false;
        }
        record.setStatus(JobStatus.RUNNING);
        record.setStartedAt(LocalDateTime.now());
        record.setLastHeartbeat(LocalDateTime.now());
        repository.save(record);
        return true;
    }
}
