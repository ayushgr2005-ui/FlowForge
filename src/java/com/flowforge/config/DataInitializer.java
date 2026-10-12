package com.flowforge.config;

import com.flowforge.model.*;
import com.flowforge.repository.*;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class DataInitializer implements CommandLineRunner {

    private final WorkflowRepository workflowRepo;
    private final JobRepository jobRepo;
    private final JobDependencyRepository depRepo;

    public DataInitializer(WorkflowRepository workflowRepo, JobRepository jobRepo, JobDependencyRepository depRepo) {
        this.workflowRepo = workflowRepo;
        this.jobRepo = jobRepo;
        this.depRepo = depRepo;
    }

    @Override
    public void run(String... args) {
        if (workflowRepo.count() == 0) {
            Workflow wf = workflowRepo.save(new Workflow("Distributed ETL & ML Pipeline"));

            // 1. Root Ingestion
            Job jobA = jobRepo.save(new Job(wf.getId(), "A: Data Ingestion Engine", Enums.JobType.LOG, "RAW_STREAM_INGESTED"));

            // 2. Parallel Processing Branches
            Job jobB = jobRepo.save(new Job(wf.getId(), "B: Feature Compute", Enums.JobType.COMPUTE, "VECTOR_EMBEDDING_1024D"));
            Job jobC = jobRepo.save(new Job(wf.getId(), "C: Token Normalizer", Enums.JobType.TRANSFORM, "TEXT_TRANSFORM_LOWER"));

            // 3. Downstream Aggregator
            Job jobD = jobRepo.save(new Job(wf.getId(), "D: External Sync Dispatch", Enums.JobType.HTTP_DISPATCH, "https://api.tier1.io/v1/sync"));

            // Graph topology: A -> B, A -> C, (B + C) -> D
            depRepo.save(new JobDependency(jobB.getId(), jobA.getId()));
            depRepo.save(new JobDependency(jobC.getId(), jobA.getId()));
            depRepo.save(new JobDependency(jobD.getId(), jobB.getId()));
            depRepo.save(new JobDependency(jobD.getId(), jobC.getId()));
        }
    }
}