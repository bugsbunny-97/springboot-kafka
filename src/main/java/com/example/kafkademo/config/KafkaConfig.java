package com.example.kafkademo.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    /**
     * Ensures the topic exists (created via KafkaAdmin on startup).
     * Use 1 replica locally, 3 on MSK (see KAFKA_TOPIC_REPLICAS).
     */
    @Bean
    public NewTopic messagesTopic(@Value("${app.topic.name}") String name,
                                  @Value("${app.topic.partitions}") int partitions,
                                  @Value("${app.topic.replicas}") int replicas) {
        return TopicBuilder.name(name)
                .partitions(partitions)
                .replicas(replicas)
                .build();
    }

    /**
     * Retries a failed record twice (1s apart); if it still fails, it is
     * published to "<topic>.DLT" (DeadLetterPublishingRecoverer default).
     * Spring Boot auto-configuration injects this CommonErrorHandler bean
     * into the listener container factory.
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> template) {
        return new DefaultErrorHandler(
                new DeadLetterPublishingRecoverer(template),
                new FixedBackOff(1000L, 2));
    }
}
