package com.flowforge.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.kafka.JobEventPayload;
import com.flowforge.kafka.JobEventProducer;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Set;

@Service
public class DelayedRetryQueueService {

    private static final String RETRY_ZSET_KEY = "flowforge:retry:delayed_queue";
    private final StringRedisTemplate redisTemplate;
    private final JobEventProducer producer;
    private final ObjectMapper mapper;

    public DelayedRetryQueueService(StringRedisTemplate redisTemplate, JobEventProducer producer, ObjectMapper mapper) {
        this.redisTemplate = redisTemplate;
        this.producer = producer;
        this.mapper = mapper;
    }

    public void scheduleRetry(JobEventPayload payload, long delayMillis) {
        try {
            double triggerTime = Instant.now().toEpochMilli() + delayMillis;
            String serialized = mapper.writeValueAsString(payload);
            redisTemplate.opsForZSet().add(RETRY_ZSET_KEY, serialized, triggerTime);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to schedule durable retry", e);
        }
    }

    /** Legacy Redis drain retained for migration only; deliberately not scheduled. */
    public void drainDueRetries() {
        double now = Instant.now().toEpochMilli();
        Set<ZSetOperations.TypedTuple<String>> dueTasks = 
                redisTemplate.opsForZSet().rangeByScoreWithScores(RETRY_ZSET_KEY, 0, now);

        if (dueTasks == null || dueTasks.isEmpty()) return;

        for (ZSetOperations.TypedTuple<String> tuple : dueTasks) {
            String value = tuple.getValue();
            if (value != null) {
                try {
                    JobEventPayload payload = mapper.readValue(value, JobEventPayload.class);
                    producer.enqueueOutbox(payload.executionId(), payload.jobId(), payload.retryAttempt(), payload.inputContext());
                    redisTemplate.opsForZSet().remove(RETRY_ZSET_KEY, value);
                } catch (Exception e) {
                    System.err.println("Error re-enqueueing retry task: " + e.getMessage());
                }
            }
        }
    }
}