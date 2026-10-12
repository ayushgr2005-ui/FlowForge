package com.flowforge.service;

import com.flowforge.kafka.JobEventProducer;
import com.flowforge.model.*;
import com.flowforge.model.Enums.ExecutionStatus;
import com.flowforge.model.Enums.JobStatus;
import com.flowforge.repository.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class ExecutionCoordinator {

    private final WorkflowExecutionRepository workflowExecutionRepo;
    private final WorkflowRepository workflowRepo;
    private final JobExecutionRepository jobExecutionRepo;
    private final JobRepository jobRepo;
    private final JobDependencyRepository dependencyRepo;
    private final JobEventProducer producer;
    private final DagValidationService dagValidator;

    public ExecutionCoordinator(WorkflowExecutionRepository workflowExecutionRepo,
                                WorkflowRepository workflowRepo,
                                JobExecutionRepository jobExecutionRepo,
                                JobRepository jobRepo,
                                JobDependencyRepository dependencyRepo,
                                JobEventProducer producer,
                                DagValidationService dagValidator) {
        this.workflowExecutionRepo = workflowExecutionRepo;
        this.workflowRepo = workflowRepo;
        this.jobExecutionRepo = jobExecutionRepo;
        this.jobRepo = jobRepo;
        this.dependencyRepo = dependencyRepo;
        this.producer = producer;
        this.dagValidator = dagValidator;
    }

    @Transactional
    public Long triggerWorkflow(Long workflowId) {
        if (!workflowRepo.existsById(workflowId)) {
            throw new IllegalArgumentException("Workflow " + workflowId + " was not found");
        }
        List<Job> jobs = jobRepo.findByWorkflowId(workflowId);
        if (jobs.isEmpty()) {
            throw new IllegalStateException("Workflow has no jobs; add at least one job before triggering it");
        }
        java.util.Set<Long> workflowJobIds = jobs.stream().map(Job::getId).collect(java.util.stream.Collectors.toSet());
        List<JobDependency> allDependencies = dependencyRepo.findAll();
        List<JobDependency> dependencies = allDependencies.stream()
                .filter(d -> workflowJobIds.contains(d.getJobId())
                        || workflowJobIds.contains(d.getDependsOnJobId()))
                .toList();

        // Reject dependencies crossing workflow boundaries instead of silently filtering them out.
        for (JobDependency dependency : dependencies) {
            boolean childInWorkflow = workflowJobIds.contains(dependency.getJobId());
            boolean parentInWorkflow = workflowJobIds.contains(dependency.getDependsOnJobId());
            if (childInWorkflow != parentInWorkflow) {
                throw new IllegalArgumentException(
                        "Dependency crosses workflow boundary: job " + dependency.getJobId()
                                + " depends on job " + dependency.getDependsOnJobId());
            }
        }

        // Step 1: Validate DAG (zero cycles allowed)
        dagValidator.validateAcyclic(jobs, dependencies);

        // Step 2: Initialize Workflow Execution Run
        WorkflowExecution exec = workflowExecutionRepo.save(new WorkflowExecution(workflowId, ExecutionStatus.RUNNING));

        for (Job job : jobs) {
            List<JobDependency> jobDeps = dependencyRepo.findByJobId(job.getId());
            boolean isRoot = jobDeps.isEmpty();
            JobStatus initialStatus = isRoot ? JobStatus.READY : JobStatus.BLOCKED;

            jobExecutionRepo.save(new JobExecution(exec.getId(), job.getId(), initialStatus));

            if (isRoot) {
                producer.dispatch(exec.getId(), job.getId(), 0, "INIT_SEED_DATA");
            }
        }
        return exec.getId();
    }

    @Transactional
    public void onJobSuccess(Long executionId, Long finishedJobId, String outputData) {
        JobExecution currentJob = jobExecutionRepo.findByExecutionIdAndJobId(executionId, finishedJobId).orElseThrow();
        currentJob.setStatus(JobStatus.SUCCESS);
        currentJob.setOutputData(outputData);
        currentJob.setCompletedAt(LocalDateTime.now());
        jobExecutionRepo.save(currentJob);

        // Step 3: Dynamic Child Unblocking with Upstream Output Aggregation
        List<JobDependency> childLinks = dependencyRepo.findByDependsOnJobId(finishedJobId);

        for (JobDependency link : childLinks) {
            Long childJobId = link.getJobId();
            JobExecution childExec = jobExecutionRepo.findByExecutionIdAndJobId(executionId, childJobId).orElse(null);

            if (childExec != null && childExec.getStatus() == JobStatus.BLOCKED) {
                if (areAllParentsDone(executionId, childJobId)) {
                    childExec.setStatus(JobStatus.READY);
                    jobExecutionRepo.save(childExec);

                    // Context aggregation: Combine outputs from all upstream parent nodes
                    String aggregatedContext = getUpstreamOutputs(executionId, childJobId);
                    producer.dispatch(executionId, childJobId, 0, aggregatedContext);
                }
            }
        }

        checkWorkflowResolution(executionId);
    }

    @Transactional
    public void onJobTerminalFailure(Long executionId, Long jobId) {
        JobExecution currentJob = jobExecutionRepo.findByExecutionIdAndJobId(executionId, jobId)
                .orElseThrow();
        currentJob.setStatus(JobStatus.DEAD_LETTER);
        currentJob.setCompletedAt(LocalDateTime.now());
        jobExecutionRepo.save(currentJob);
        checkWorkflowResolution(executionId);
    }

    private boolean areAllParentsDone(Long executionId, Long childJobId) {
        List<JobDependency> parents = dependencyRepo.findByJobId(childJobId);
        for (JobDependency p : parents) {
            JobExecution parentExec = jobExecutionRepo.findByExecutionIdAndJobId(executionId, p.getDependsOnJobId()).orElse(null);
            if (parentExec == null || parentExec.getStatus() != JobStatus.SUCCESS) {
                return false;
            }
        }
        return true;
    }

    private String getUpstreamOutputs(Long executionId, Long childJobId) {
        List<JobDependency> parents = dependencyRepo.findByJobId(childJobId);
        return parents.stream()
                .map(p -> jobExecutionRepo.findByExecutionIdAndJobId(executionId, p.getDependsOnJobId()).orElse(null))
                .filter(p -> p != null && p.getOutputData() != null)
                .map(JobExecution::getOutputData)
                .collect(Collectors.joining(" | "));
    }

    private void checkWorkflowResolution(Long executionId) {
        List<JobExecution> all = jobExecutionRepo.findByExecutionId(executionId);
        boolean anyFailed = all.stream().anyMatch(j -> j.getStatus() == JobStatus.FAILED || j.getStatus() == JobStatus.DEAD_LETTER);
        boolean allSuccess = all.stream().allMatch(j -> j.getStatus() == JobStatus.SUCCESS);

        WorkflowExecution wf = workflowExecutionRepo.findById(executionId).orElseThrow();
        if (anyFailed) {
            wf.setStatus(ExecutionStatus.FAILED);
            wf.setEndedAt(LocalDateTime.now());
            workflowExecutionRepo.save(wf);
        } else if (allSuccess) {
            wf.setStatus(ExecutionStatus.COMPLETED);
            wf.setEndedAt(LocalDateTime.now());
            workflowExecutionRepo.save(wf);
        }
    }
}