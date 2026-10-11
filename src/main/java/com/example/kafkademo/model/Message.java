package com.example.kafkademo.model;

/**
 * Event payload exchanged over Kafka as JSON.
 */
public record Message(String id, String content) {
}
