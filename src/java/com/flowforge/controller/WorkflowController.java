package com.flowforge.controller;

import com.flowforge.model.*;
import com.flowforge.repository.*;
import com.flowforge.service.ExecutionCoordinator;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class WorkflowController {

    private final WorkflowRepository workflowRepo;
    private final JobRepository jobRepo;
    private final JobDependencyRepository depRepo;
    private final JobExecutionRepository jobExecutionRepo;
    private final ExecutionCoordinator coordinator;

    public WorkflowController(WorkflowRepository workflowRepo, JobRepository jobRepo,
                              JobDependencyRepository depRepo, JobExecutionRepository jobExecutionRepo,
                              ExecutionCoordinator coordinator) {
        this.workflowRepo = workflowRepo;
        this.jobRepo = jobRepo;
        this.depRepo = depRepo;
        this.jobExecutionRepo = jobExecutionRepo;
        this.coordinator = coordinator;
    }

    @GetMapping("/workflows")
    public List<Workflow> listWorkflows() {
        return workflowRepo.findAll();
    }

    @GetMapping("/workflows/{workflowId}/jobs")
    public ResponseEntity<List<Job>> listJobs(@PathVariable Long workflowId) {
        if (!workflowRepo.existsById(workflowId)) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(jobRepo.findByWorkflowId(workflowId));
    }

    @PostMapping("/workflows")
    public ResponseEntity<Workflow> createWorkflow(@RequestBody Map<String, String> body) {
        return ResponseEntity.ok(workflowRepo.save(new Workflow(body.get("name"))));
    }

    @PostMapping("/jobs")
    public ResponseEntity<Job> createJob(@RequestBody Job job) {
        return ResponseEntity.ok(jobRepo.save(job));
    }

    @PostMapping("/dependencies")
    public ResponseEntity<JobDependency> addDependency(@RequestBody JobDependency dep) {
        return ResponseEntity.ok(depRepo.save(dep));
    }

    @PostMapping("/executions/trigger/{workflowId}")
    public ResponseEntity<?> trigger(@PathVariable Long workflowId) {
        try {
            Long executionId = coordinator.triggerWorkflow(workflowId);
            return ResponseEntity.ok(Map.of("executionId", executionId, "status", "TRIGGERED"));
        } catch (IllegalStateException | IllegalArgumentException validationEx) {
            HttpStatus status = validationEx.getMessage() != null && validationEx.getMessage().contains("not found")
                    ? HttpStatus.NOT_FOUND : HttpStatus.BAD_REQUEST;
            return ResponseEntity.status(status).body(Map.of("error", validationEx.getMessage()));
        }
    }

    @GetMapping("/executions/{executionId}/status")
    public ResponseEntity<List<JobExecution>> getStatus(@PathVariable Long executionId) {
        return ResponseEntity.ok(jobExecutionRepo.findByExecutionId(executionId));
    }
}