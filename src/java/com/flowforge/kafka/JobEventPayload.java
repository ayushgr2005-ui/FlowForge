package com.flowforge.kafka;

public record JobEventPayload(
    Long executionId,
    Long jobId,
    int retryAttempt,
    String inputContext
) {}