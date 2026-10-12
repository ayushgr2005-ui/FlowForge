package com.flowforge.model;

import jakarta.persistence.*;

@Entity
@Table(name = "jobs")
public class Job {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private Long workflowId;
    private String name;

    @Enumerated(EnumType.STRING)
    private Enums.JobType type;
    private String payload;

    public Job() {}
    public Job(Long workflowId, String name, Enums.JobType type, String payload) {
        this.workflowId = workflowId;
        this.name = name;
        this.type = type;
        this.payload = payload;
    }

    public Long getId() { return id; }
    public Long getWorkflowId() { return workflowId; }
    public String getName() { return name; }
    public Enums.JobType getType() { return type; }
    public String getPayload() { return payload; }
}