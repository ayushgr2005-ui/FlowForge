package com.flowforge.model;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "outbox_events")
public class OutboxEvent {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String aggregateType;
    private String aggregateId;
    private String topic;

    @Column(columnDefinition = "TEXT")
    private String payload;

    private boolean processed = false;
    private LocalDateTime createdAt = LocalDateTime.now();
    private LocalDateTime availableAt = LocalDateTime.now();

    public OutboxEvent() {}

    public OutboxEvent(String aggregateType, String aggregateId, String topic, String payload) {
        this(aggregateType, aggregateId, topic, payload, LocalDateTime.now());
    }

    public OutboxEvent(String aggregateType, String aggregateId, String topic, String payload,
                       LocalDateTime availableAt) {
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.topic = topic;
        this.payload = payload;
        this.availableAt = availableAt == null ? LocalDateTime.now() : availableAt;
    }

    public Long getId() { return id; }
    public String getTopic() { return topic; }
    public String getAggregateId() { return aggregateId; }
    public String getPayload() { return payload; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public LocalDateTime getAvailableAt() { return availableAt; }
    public void setAvailableAt(LocalDateTime availableAt) { this.availableAt = availableAt; }
    public boolean isProcessed() { return processed; }
    public void setProcessed(boolean processed) { this.processed = processed; }
}