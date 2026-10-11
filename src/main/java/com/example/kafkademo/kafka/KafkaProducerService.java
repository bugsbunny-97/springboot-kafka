package com.example.kafkademo.kafka;

import com.example.kafkademo.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);

    private final KafkaTemplate<String, Message> kafkaTemplate;
    private final String topic;

    public KafkaProducerService(KafkaTemplate<String, Message> kafkaTemplate,
                                @Value("${app.topic.name}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void send(Message message) {
        kafkaTemplate.send(topic, message.id(), message)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Sent id={} to {} [partition={}, offset={}]",
                                message.id(), topic,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        log.error("Failed to send id={} to {}", message.id(), topic, ex);
                    }
                });
    }
}
