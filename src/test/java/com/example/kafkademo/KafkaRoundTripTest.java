package com.example.kafkademo;

import com.example.kafkademo.kafka.KafkaProducerService;
import com.example.kafkademo.model.Message;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Full round-trip test against an in-process Kafka broker.
 * @EmbeddedKafka re-points spring.kafka.bootstrap-servers at the embedded broker.
 */
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = "${app.topic.name}")
class KafkaRoundTripTest {

    @Autowired
    private KafkaProducerService producerService;

    @Autowired
    private DefaultKafkaConsumerFactory<String, Message> consumerFactory;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Value("${app.topic.name}")
    private String topic;

    @Test
    void messageProducedByServiceIsConsumableFromTopic() {
        producerService.send(new Message("id-1", "hello kafka"));

        Consumer<String, Message> consumer = consumerFactory.createConsumer("test-group", "");
        try {
            consumer.subscribe(List.of(topic));
            ConsumerRecord<String, Message> record = KafkaTestUtils.getSingleRecord(consumer, topic);

            assertEquals("id-1", record.key());
            assertEquals("hello kafka", record.value().content());
        } finally {
            consumer.close();
        }
    }

    @Test
    void embeddedBrokerIsRunning() {
        assertEquals(1, embeddedKafka.getBrokersAsString().split(",").length);
    }
}
