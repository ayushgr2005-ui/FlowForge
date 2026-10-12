package com.flowforge.service;

import com.flowforge.model.Enums.JobStatus;
import com.flowforge.model.JobExecution;
import com.flowforge.repository.JobExecutionRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class JobExecutionClaimServiceTest {
    @Mock
    private JobExecutionRepository repository;

    @InjectMocks
    private JobExecutionClaimService claimService;

    @Test
    void claimsReadyJobExactlyOnce() {
        JobExecution record = new JobExecution(10L, 20L, JobStatus.READY);
        when(repository.findForUpdateByExecutionIdAndJobId(10L, 20L))
                .thenReturn(Optional.of(record));

        assertTrue(claimService.claim(10L, 20L, 0));
        assertEquals(JobStatus.RUNNING, record.getStatus());
        assertFalse(claimService.claim(10L, 20L, 0));
        verify(repository, times(2)).findForUpdateByExecutionIdAndJobId(10L, 20L);
    }

    @Test
    void retryCanClaimOnlyAfterRetryStateWasCommitted() {
        JobExecution record = new JobExecution(10L, 20L, JobStatus.READY);
        record.setRetryCount(1);
        when(repository.findForUpdateByExecutionIdAndJobId(10L, 20L))
                .thenReturn(Optional.of(record));

        assertTrue(claimService.claim(10L, 20L, 1));
        assertEquals(1, record.getRetryCount());
        assertEquals(JobStatus.RUNNING, record.getStatus());
    }

    @Test
    void staleRetryAttemptCannotClaimNewerAttempt() {
        JobExecution record = new JobExecution(10L, 20L, JobStatus.READY);
        record.setRetryCount(2);
        when(repository.findForUpdateByExecutionIdAndJobId(10L, 20L))
                .thenReturn(Optional.of(record));

        assertFalse(claimService.claim(10L, 20L, 1));
        assertEquals(JobStatus.READY, record.getStatus());
    }

    @Test
    void duplicateRetryCannotClaimRunningJobAgain() {
        JobExecution record = new JobExecution(10L, 20L, JobStatus.RUNNING);
        record.setRetryCount(1);
        when(repository.findForUpdateByExecutionIdAndJobId(10L, 20L))
                .thenReturn(Optional.of(record));

        assertFalse(claimService.claim(10L, 20L, 1));
    }
}
