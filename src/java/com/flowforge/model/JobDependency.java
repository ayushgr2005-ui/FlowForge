package com.flowforge.model;

import jakarta.persistence.*;

@Entity
@Table(name = "job_dependencies")
public class JobDependency {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "job_id", nullable = false)
    private Long jobId;

    @Column(name = "depends_on_job_id", nullable = false)
    private Long dependsOnJobId;

    public JobDependency() {
    }

    public JobDependency(Long jobId, Long dependsOnJobId) {
        this.jobId = jobId;
        this.dependsOnJobId = dependsOnJobId;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getJobId() {
        return jobId;
    }

    public void setJobId(Long jobId) {
        this.jobId = jobId;
    }

    public Long getDependsOnJobId() {
        return dependsOnJobId;
    }

    public void setDependsOnJobId(Long dependsOnJobId) {
        this.dependsOnJobId = dependsOnJobId;
    }
}