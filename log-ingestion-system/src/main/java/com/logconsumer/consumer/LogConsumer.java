package com.logconsumer.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Profile("consumer")
public class LogConsumer {
    private static final Logger log = LoggerFactory.getLogger(LogConsumer.class);

    @KafkaListener(topics = "logs.events", containerFactory = "kafkaListenerContainerFactory")
    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.info("Consumed from partition {} key {} value {}", record.partition(), record.key(), record.value());
        ack.acknowledge();
    }
}

