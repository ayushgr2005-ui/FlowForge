package com.flowforge.model;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "workflow_executions")
public class WorkflowExecution {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private Long workflowId;

    @Enumerated(EnumType.STRING)
    private Enums.ExecutionStatus status;
    private LocalDateTime startedAt = LocalDateTime.now();
    private LocalDateTime endedAt;

    public WorkflowExecution() {}
    public WorkflowExecution(Long workflowId, Enums.ExecutionStatus status) {
        this.workflowId = workflowId;
        this.status = status;
    }

    public Long getId() { return id; }
    public Long getWorkflowId() { return workflowId; }
    public Enums.ExecutionStatus getStatus() { return status; }
    public void setStatus(Enums.ExecutionStatus status) { this.status = status; }
    public void setEndedAt(LocalDateTime endedAt) { this.endedAt = endedAt; }
}