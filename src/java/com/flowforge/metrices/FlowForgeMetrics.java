package com.flowforge.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class FlowForgeMetrics {

    private final Counter completedJobsCounter;
    private final Counter failedJobsCounter;
    private final Counter dlqJobsCounter;
    private final Timer jobExecutionTimer;

    public FlowForgeMetrics(MeterRegistry registry) {
        this.completedJobsCounter = Counter.builder("flowforge.jobs.completed")
                .description("Total successfully finished jobs")
                .register(registry);

        this.failedJobsCounter = Counter.builder("flowforge.jobs.failed")
                .description("Total failed job attempts")
                .register(registry);

        this.dlqJobsCounter = Counter.builder("flowforge.jobs.dlq")
                .description("Total jobs exhausted retries and pushed to DLQ")
                .register(registry);

        this.jobExecutionTimer = Timer.builder("flowforge.jobs.duration")
                .description("Time taken to execute job business logic")
                .register(registry);
    }

    public void incrementCompleted() { completedJobsCounter.increment(); }
    public void incrementFailed() { failedJobsCounter.increment(); }
    public void incrementDLQ() { dlqJobsCounter.increment(); }
    public void recordDuration(long millis) { jobExecutionTimer.record(millis, TimeUnit.MILLISECONDS); }
}