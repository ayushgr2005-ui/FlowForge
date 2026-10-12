package com.flowforge.model;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "job_executions", uniqueConstraints = @UniqueConstraint(name = "uk_execution_job", columnNames = {"execution_id", "job_id"}))
public class JobExecution {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    private Long executionId;
    private Long jobId;

    @Enumerated(EnumType.STRING)
    private Enums.JobStatus status;

    private int retryCount = 0;
    private int maxRetries = 3;

    @Column(columnDefinition = "TEXT")
    private String outputData;

    private LocalDateTime lastHeartbeat;
    private LocalDateTime startedAt;
    private LocalDateTime completedAt;

    public JobExecution() {}

    public JobExecution(Long executionId, Long jobId, Enums.JobStatus status) {
        this.executionId = executionId;
        this.jobId = jobId;
        this.status = status;
        this.lastHeartbeat = LocalDateTime.now();
    }

    // Getters and Setters
    public Long getId() { return id; }
    public Long getExecutionId() { return executionId; }
    public Long getJobId() { return jobId; }
    public Enums.JobStatus getStatus() { return status; }
    public void setStatus(Enums.JobStatus status) { this.status = status; }
    public int getRetryCount() { return retryCount; }
    public void setRetryCount(int retryCount) { this.retryCount = retryCount; }
    public int getMaxRetries() { return maxRetries; }
    public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }
    public String getOutputData() { return outputData; }
    public void setOutputData(String outputData) { this.outputData = outputData; }
    public LocalDateTime getLastHeartbeat() { return lastHeartbeat; }
    public void setLastHeartbeat(LocalDateTime lastHeartbeat) { this.lastHeartbeat = lastHeartbeat; }
    public LocalDateTime getStartedAt() { return startedAt; }
    public void setStartedAt(LocalDateTime startedAt) { this.startedAt = startedAt; }
    public LocalDateTime getCompletedAt() { return completedAt; }
    public void setCompletedAt(LocalDateTime completedAt) { this.completedAt = completedAt; }
}