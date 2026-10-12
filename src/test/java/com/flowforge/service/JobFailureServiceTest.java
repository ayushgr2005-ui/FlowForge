package com.flowforge.service;

import com.flowforge.kafka.JobEventProducer;
import com.flowforge.model.Enums.JobStatus;
import com.flowforge.model.JobExecution;
import com.flowforge.repository.JobExecutionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class JobFailureServiceTest {
    private JobExecutionRepository repository;
    private JobEventProducer producer;
    private ExecutionCoordinator coordinator;
    private JobFailureService service;

    @BeforeEach
    void setUp() {
        repository = mock(JobExecutionRepository.class);
        producer = mock(JobEventProducer.class);
        coordinator = mock(ExecutionCoordinator.class);
        service = new JobFailureService(repository, producer, coordinator);
    }

    @Test
    void retryStateAndOutboxEventAreWrittenForRetryableFailure() {
        JobExecution execution = new JobExecution(10L, 20L, JobStatus.RUNNING);
        execution.setMaxRetries(3);
        when(repository.findForUpdateByExecutionIdAndJobId(10L, 20L)).thenReturn(Optional.of(execution));

        JobFailureService.FailureOutcome outcome = service.recordFailure(10L, 20L, "input", "temporary failure");

        assertEquals(JobFailureService.FailureOutcome.RETRY_SCHEDULED, outcome);
        assertEquals(JobStatus.READY, execution.getStatus());
        assertEquals(1, execution.getRetryCount());
        verify(repository).save(execution);
        verify(producer).enqueueOutboxAfterDelay(eq(10L), eq(20L), eq(1), eq("input"), longThat(delay -> delay > 0L));
        verify(producer, never()).enqueueDlqOutbox(anyLong(), anyLong(), anyString());
        verifyNoInteractions(coordinator);
    }

    @Test
    void exhaustedRetriesPersistDeadLetterAndNotifyCoordinator() {
        JobExecution execution = new JobExecution(10L, 20L, JobStatus.RUNNING);
        execution.setRetryCount(3);
        execution.setMaxRetries(3);
        when(repository.findForUpdateByExecutionIdAndJobId(10L, 20L)).thenReturn(Optional.of(execution));

        JobFailureService.FailureOutcome outcome = service.recordFailure(10L, 20L, "input", "permanent failure");

        assertEquals(JobFailureService.FailureOutcome.DEAD_LETTERED, outcome);
        assertEquals(JobStatus.DEAD_LETTER, execution.getStatus());
        assertEquals(4, execution.getRetryCount());
        assertNotNull(execution.getCompletedAt());
        verify(repository).save(execution);
        verify(producer).enqueueDlqOutbox(10L, 20L, "permanent failure");
        verify(coordinator).onJobTerminalFailure(10L, 20L);
    }

    @Test
    void duplicateFailureForNonRunningJobDoesNotScheduleAnything() {
        JobExecution execution = new JobExecution(10L, 20L, JobStatus.SUCCESS);
        when(repository.findForUpdateByExecutionIdAndJobId(10L, 20L)).thenReturn(Optional.of(execution));

        JobFailureService.FailureOutcome outcome = service.recordFailure(10L, 20L, "input", "late duplicate");

        assertEquals(JobFailureService.FailureOutcome.IGNORED, outcome);
        verify(repository, never()).save(any(JobExecution.class));
        verifyNoInteractions(producer, coordinator);
    }
}
