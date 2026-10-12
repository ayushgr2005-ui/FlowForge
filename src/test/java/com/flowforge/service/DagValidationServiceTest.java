package com.flowforge.service;

import com.flowforge.model.Job;
import com.flowforge.model.JobDependency;
import com.flowforge.model.Enums;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DagValidationServiceTest {
    private final DagValidationService validator = new DagValidationService();

    private Job job(long id) {
        Job job = new Job(1L, "job-" + id, Enums.JobType.LOG, "{}");
        try {
            var field = Job.class.getDeclaredField("id");
            field.setAccessible(true);
            field.set(job, id);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
        return job;
    }

    @Test
    void acceptsAcyclicGraph() {
        assertDoesNotThrow(() -> validator.validateAcyclic(
                List.of(job(1), job(2), job(3)),
                List.of(new JobDependency(2L, 1L), new JobDependency(3L, 2L))));
    }

    @Test
    void rejectsCycle() {
        assertThrows(IllegalStateException.class, () -> validator.validateAcyclic(
                List.of(job(1), job(2), job(3)),
                List.of(new JobDependency(2L, 1L), new JobDependency(3L, 2L),
                        new JobDependency(1L, 3L))));
    }

    @Test
    void acceptsIndependentJobs() {
        assertDoesNotThrow(() -> validator.validateAcyclic(
                List.of(job(1), job(2)), List.of()));
    }

    
@Test
void rejectsMissingJobReference() {
    assertThrows(IllegalArgumentException.class, () ->
            validator.validateAcyclic(
                    List.of(job(1)),
                    List.of(new JobDependency(2L, 1L))
            ));
}

@Test
void rejectsDuplicateDependency() {
    assertThrows(IllegalArgumentException.class, () ->
            validator.validateAcyclic(
                    List.of(job(1), job(2)),
                    List.of(
                            new JobDependency(2L, 1L),
                            new JobDependency(2L, 1L)
                    )
            ));
}

@Test
void rejectsSelfDependency() {
    assertThrows(IllegalStateException.class, () ->
            validator.validateAcyclic(
                    List.of(job(1)),
                    List.of(new JobDependency(1L, 1L))
            ));
}

@Test
void rejectsDuplicateJobIds() {
    assertThrows(IllegalArgumentException.class, () ->
            validator.validateAcyclic(
                    List.of(job(1), job(1)),
                    List.of()
            ));
}

@Test
void rejectsNullJobsList() {
    assertThrows(IllegalArgumentException.class, () ->
            validator.validateAcyclic(null, List.of()));
}

@Test
void acceptsEmptyWorkflow() {
    assertDoesNotThrow(() ->
            validator.validateAcyclic(List.of(), List.of()));
}

}
