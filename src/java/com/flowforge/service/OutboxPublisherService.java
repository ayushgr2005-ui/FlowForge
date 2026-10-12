package com.flowforge.service;

import com.flowforge.model.OutboxEvent;
import com.flowforge.repository.OutboxEventRepository;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

@Service
public class OutboxPublisherService {
    private final OutboxEventRepository outboxRepo;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxPublisherService(OutboxEventRepository outboxRepo,
                                  KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxRepo = outboxRepo;
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Row locks prevent concurrent app instances from publishing the same pending
     * rows at once. Wait for the broker acknowledgement before marking processed.
     * A crash after broker ack but before DB commit can still redeliver; consumers
     * therefore remain idempotent by design.
     */
    @Scheduled(fixedDelay = 500)
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> events = outboxRepo.findPendingForUpdate(LocalDateTime.now(), PageRequest.of(0, 50));
        for (OutboxEvent event : events) {
            try {
                kafkaTemplate.send(event.getTopic(), event.getAggregateId(), event.getPayload())
                        .get(10, TimeUnit.SECONDS);
                event.setProcessed(true);
                outboxRepo.save(event);
            } catch (Exception publishError) {
                // Keep the row pending. A later scheduler tick retries it.
                System.err.println("Outbox event " + event.getId()
                        + " publish failed; will retry: " + publishError.getMessage());
                break;
            }
        }
    }
}
