package com.example.kafkademo.controller;

import com.example.kafkademo.kafka.KafkaProducerService;
import com.example.kafkademo.model.Message;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/messages")
public class MessageController {

    private final KafkaProducerService producer;

    public MessageController(KafkaProducerService producer) {
        this.producer = producer;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, String> publish(@RequestBody Message message) {
        Message outbound = message.id() == null
                ? new Message(UUID.randomUUID().toString(), message.content())
                : message;
        producer.send(outbound);
        return Map.of("id", outbound.id(), "status", "queued");
    }
}
