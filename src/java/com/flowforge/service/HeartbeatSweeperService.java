package com.flowforge.service;

import com.flowforge.kafka.JobEventProducer;
import com.flowforge.model.Enums.JobStatus;
import com.flowforge.model.JobExecution;
import com.flowforge.repository.JobExecutionRepository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class HeartbeatSweeperService {
    private final JobExecutionRepository jobExecutionRepo;
    private final JobEventProducer producer;

    public HeartbeatSweeperService(JobExecutionRepository jobExecutionRepo, JobEventProducer producer) {
        this.jobExecutionRepo = jobExecutionRepo;
        this.producer = producer;
    }

    /**
     * Recovers stale RUNNING jobs under database row locks. The READY transition
     * and outbox insert are committed together, avoiding duplicate schedulers.
     */
    @Scheduled(fixedRate = 15000)
    @Transactional
    public void sweepZombieJobs() {
        LocalDateTime cutoff = LocalDateTime.now().minusSeconds(45);
        List<JobExecution> zombies = jobExecutionRepo.findZombieJobsForUpdate(cutoff);

        for (JobExecution zombie : zombies) {
            if (zombie.getStatus() != JobStatus.RUNNING
                    || zombie.getLastHeartbeat() == null
                    || !zombie.getLastHeartbeat().isBefore(cutoff)) {
                continue;
            }
            System.err.printf("[ORPHAN RECOVERY] Recovering stale Job %d.%n", zombie.getJobId());
            zombie.setStatus(JobStatus.READY);
            zombie.setLastHeartbeat(LocalDateTime.now());
            jobExecutionRepo.save(zombie);
            producer.dispatch(zombie.getExecutionId(), zombie.getJobId(),
                    zombie.getRetryCount(), "RECOVERED_FROM_CRASH");
        }
    }
}
