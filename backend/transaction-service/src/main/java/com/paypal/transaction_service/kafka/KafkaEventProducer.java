package com.paypal.transaction_service.kafka;

import com.paypal.transaction_service.entity.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaEventProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventProducer.class);
    private static final String TOPIC = "transaction-events";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendTransactionEvent(String key, Transaction transaction) {
        kafkaTemplate.send(TOPIC, key, transaction)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send Kafka event for key {}: {}", key, ex.getMessage());
                    } else {
                        log.info("Kafka event sent to topic {} partition {} offset {}",
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }
}
