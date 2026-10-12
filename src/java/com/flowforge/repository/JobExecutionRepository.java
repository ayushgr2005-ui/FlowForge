package com.flowforge.repository;

import com.flowforge.model.JobExecution;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.Lock;
import jakarta.persistence.LockModeType;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface JobExecutionRepository extends JpaRepository<JobExecution, Long> {
    
    List<JobExecution> findByExecutionId(Long executionId);
    
    Optional<JobExecution> findByExecutionIdAndJobId(Long executionId, Long jobId);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT j FROM JobExecution j WHERE j.executionId = :executionId AND j.jobId = :jobId")
    Optional<JobExecution> findForUpdateByExecutionIdAndJobId(
            @Param("executionId") Long executionId, @Param("jobId") Long jobId);

    @Query("SELECT j FROM JobExecution j WHERE j.status = 'RUNNING' AND j.lastHeartbeat < :cutoff")
    List<JobExecution> findZombieJobs(@Param("cutoff") LocalDateTime cutoff);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT j FROM JobExecution j WHERE j.status = 'RUNNING' AND j.lastHeartbeat < :cutoff")
    List<JobExecution> findZombieJobsForUpdate(@Param("cutoff") LocalDateTime cutoff);
}