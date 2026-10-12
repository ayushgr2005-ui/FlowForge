package com.flowforge.repository;

import com.flowforge.model.JobDependency;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface JobDependencyRepository extends JpaRepository<JobDependency, Long> {
    List<JobDependency> findByJobId(Long jobId);
    List<JobDependency> findByDependsOnJobId(Long dependsOnJobId);
}