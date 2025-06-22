//package com.logconsumer.consumer;
//
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.context.annotation.Profile;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.kafka.support.Acknowledgment;
//import org.springframework.stereotype.Component;
//
//@Component
//@Profile("consumer")
//public class LogConsumer {
//    private static final Logger log = LoggerFactory.getLogger(LogConsumer.class);
//
//    @KafkaListener(
//    	    topics = "logs.events", 
//    	    groupId = "log-consumer-group",  // Explicit group ID
//    	    containerFactory = "kafkaListenerContainerFactory"
//    	)
//    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
//        log.info("Consumed from partition {} key {} value {}", record.partition(), record.key(), record.value());
//        ack.acknowledge();
//    }
//}


package com.logconsumer.consumer;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;

@Component
@Profile("consumer")
public class LogConsumer {
    private static final Logger log = LoggerFactory.getLogger(LogConsumer.class);
    
    private final AtomicLong messageCount = new AtomicLong(0);
    private volatile boolean isRunning = false;

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        isRunning = true;
        log.info("Consumer is ready to consume messages from topic: logs.events");
        log.info("Consumer instance: {}", System.getProperty("consumer.instance.id", "unknown"));
    }

    @KafkaListener(
        topics = "logs.events", 
        groupId = "log-consumer-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
            ConsumerRecord<String, String> record, 
            Acknowledgment ack,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {
        
        if (!isRunning) {
            log.warn("Consumer not ready, ignoring message");
            return;
        }
        
        try {
            String consumerInstance = System.getProperty("consumer.instance.id", "unknown");
            long currentCount = messageCount.incrementAndGet();
            
            log.info("Consumer[{}] - Partition: {} | Offset: {} | Key: {} | Count: {} | Message: {}", 
                    consumerInstance, partition, offset, record.key(), currentCount, 
                    record.value().substring(0, Math.min(50, record.value().length())) + "...");
            
            // Simulate processing time
            Thread.sleep(50);
            
            // Manual acknowledgment
            ack.acknowledge();
            
            // Log every 100 messages
            if (currentCount % 100 == 0) {
                log.info("Consumer[{}] processed {} messages so far", consumerInstance, currentCount);
            }
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Processing interrupted for partition {}", partition, e);
        } catch (Exception e) {
            log.error("Error processing message from partition {} at offset {}", partition, offset, e);
            // Still acknowledge to avoid infinite retry
            ack.acknowledge();
        }
    }
    
    @PreDestroy
    public void shutdown() {
        isRunning = false;
        log.info("Consumer shutdown initiated. Total messages processed: {}", messageCount.get());
    }
}

