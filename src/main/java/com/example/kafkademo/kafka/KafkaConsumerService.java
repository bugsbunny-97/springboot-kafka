package com.example.kafkademo.kafka;

import com.example.kafkademo.model.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);

    @KafkaListener(topics = "${app.topic.name}")
    public void onMessage(ConsumerRecord<String, Message> record) {
        log.info("Received: key={}, partition={}, offset={}, payload={}",
                record.key(), record.partition(), record.offset(), record.value());
        // Business logic goes here (persist, enrich, forward, ...)
    }
}
