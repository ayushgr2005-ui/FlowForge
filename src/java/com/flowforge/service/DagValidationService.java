
package com.flowforge.service;

import com.flowforge.model.Job;
import com.flowforge.model.JobDependency;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class DagValidationService {

    /**
     * Kahn's Algorithm (Topological Sort).
     * Rejects cycles and invalid dependency references.
     */
    public void validateAcyclic(List<Job> jobs, List<JobDependency> dependencies) {

        if (jobs == null) {
            throw new IllegalArgumentException("Jobs list cannot be null.");
        }

        if (dependencies == null) {
            throw new IllegalArgumentException("Dependencies list cannot be null.");
        }

        if (jobs.isEmpty()) {
            if (!dependencies.isEmpty()) {
                throw new IllegalArgumentException(
                        "Dependencies cannot exist without jobs.");
            }
            return;
        }

        Map<Long, Integer> inDegree = new HashMap<>();
        Map<Long, List<Long>> adjacencyList = new HashMap<>();

        // Step 1: Register every job and reject duplicate job IDs.
        for (Job job : jobs) {
            if (job == null || job.getId() == null) {
                throw new IllegalArgumentException(
                        "Job and job ID cannot be null.");
            }

            Long jobId = job.getId();

            if (inDegree.containsKey(jobId)) {
                throw new IllegalArgumentException(
                        "Duplicate job ID: " + jobId);
            }

            inDegree.put(jobId, 0);
            adjacencyList.put(jobId, new ArrayList<>());
        }

        // Step 2: Validate dependencies and reject duplicates.
        Set<String> seenDependencies = new HashSet<>();

        for (JobDependency dep : dependencies) {
            if (dep == null
                    || dep.getJobId() == null
                    || dep.getDependsOnJobId() == null) {
                throw new IllegalArgumentException(
                        "Dependency and its job IDs cannot be null.");
            }

            Long parent = dep.getDependsOnJobId();
            Long child = dep.getJobId();

            if (!adjacencyList.containsKey(parent)
                    || !adjacencyList.containsKey(child)) {
                throw new IllegalArgumentException(
                        "Dependency references a job outside this workflow.");
            }

            String edgeKey = parent + "->" + child;

            if (!seenDependencies.add(edgeKey)) {
                throw new IllegalArgumentException(
                        "Duplicate dependency: " + edgeKey);
            }

            adjacencyList.get(parent).add(child);
            inDegree.put(child, inDegree.get(child) + 1);
        }

        // Step 3: Add all jobs with zero incoming dependencies.
        Queue<Long> queue = new LinkedList<>();

        for (Map.Entry<Long, Integer> entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) {
                queue.add(entry.getKey());
            }
        }

        // Step 4: Perform topological sorting.
        int visitedCount = 0;

        while (!queue.isEmpty()) {
            Long current = queue.poll();
            visitedCount++;

            for (Long neighbor : adjacencyList.get(current)) {
                int newDegree = inDegree.get(neighbor) - 1;
                inDegree.put(neighbor, newDegree);

                if (newDegree == 0) {
                    queue.add(neighbor);
                }
            }
        }

        // Step 5: If some jobs remain, the graph contains a cycle.
        if (visitedCount != jobs.size()) {
            throw new IllegalStateException(
                    "Cyclic dependency detected. Workflow execution rejected.");
        }
    }
}
