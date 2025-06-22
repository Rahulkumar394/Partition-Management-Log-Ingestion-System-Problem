//package com.log.producer;
//
//import java.time.Instant;
//import java.util.Random;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.context.annotation.Profile;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.scheduling.annotation.Scheduled;
//import org.springframework.stereotype.Component;
//
//@Component
//@Profile("producer")
//public class LogProducer {
//	private static final Logger log = LoggerFactory.getLogger(LogProducer.class);
//	private static final String TOPIC = "logs.events";
//	private final KafkaTemplate<String, String> kafkaTemplate;
//	private final Random random = new Random();
//
//	public LogProducer(KafkaTemplate<String, String> kafkaTemplate) {
//		this.kafkaTemplate = kafkaTemplate;
//	}
//
//	@Scheduled(fixedRate = 1000)
//	public void produceLog() {
//		try {
//			String appName = "app-" + random.nextInt(100);
//			String logMessage = "Log from " + appName + " at " + Instant.now();
//
//			kafkaTemplate.send(TOPIC, appName, logMessage).whenComplete((result, ex) -> {
//				if (ex == null) {
//					log.info("Produced log for key {} partition {}", appName, result.getRecordMetadata().partition());
//				} else {
//					log.error("Failed to produce log", ex);
//				}
//			});
//
//		} catch (Exception e) {
//			log.error("Error producing log", e);
//		}
//	}
//
//}

package com.log.producer;

import java.time.Instant;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;

@Component
@Profile("producer")
public class LogProducer {
    private static final Logger log = LoggerFactory.getLogger(LogProducer.class);
    private static final String TOPIC = "logs.events";
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final Random random = new Random();
    private final MeterRegistry meterRegistry;
    
    private Counter messagesSentCounter;
    private Counter messagesFailedCounter;
    private Timer sendTimer;
    private final AtomicLong partitionDistribution = new AtomicLong(0);

    public LogProducer(KafkaTemplate<String, String> kafkaTemplate, MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.meterRegistry = meterRegistry;
    }
    
    @PostConstruct
    public void initMetrics() {
        messagesSentCounter = Counter.builder("kafka.producer.messages.sent")
                .description("Total number of messages sent")
                .register(meterRegistry);
                
        messagesFailedCounter = Counter.builder("kafka.producer.messages.failed")
                .description("Total number of failed messages")
                .register(meterRegistry);
                
        sendTimer = Timer.builder("kafka.producer.send.duration")
                .description("Time taken to send messages")
                .register(meterRegistry);
    }

    @Scheduled(fixedRate = 1000) // Send every second
    public void produceLog() {
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try {
            // Generate app name (key for partitioning)
            String appName = "app-" + String.format("%03d", random.nextInt(100)); // app-000 to app-099
            String logMessage = createLogMessage(appName);

            kafkaTemplate.send(TOPIC, appName, logMessage).whenComplete((result, ex) -> {
                sample.stop(sendTimer);
                
                if (ex == null) {
                    messagesSentCounter.increment();
                    int partition = result.getRecordMetadata().partition();
                    log.info("✅ Sent to partition {} | Key: {} | Message: {}", 
                            partition, appName, logMessage);
                    partitionDistribution.incrementAndGet();
                } else {
                    messagesFailedCounter.increment();
                    log.error("❌ Failed to send message for key {}: {}", appName, ex.getMessage());
                }
            });

        } catch (Exception e) {
            sample.stop(sendTimer);
            messagesFailedCounter.increment();
            log.error("Error producing log", e);
        }
    }
    
    private String createLogMessage(String appName) {
        String[] logLevels = {"INFO", "WARN", "ERROR", "DEBUG"};
        String[] actions = {"UserLogin", "PaymentProcessed", "DataSync", "CacheUpdate", "ApiCall"};
        
        String level = logLevels[random.nextInt(logLevels.length)];
        String action = actions[random.nextInt(actions.length)];
        
        return String.format("[%s] %s - %s executed at %s by %s", 
                level, action, action, Instant.now(), appName);
    }
    
    // Method to check partition distribution
    @Scheduled(fixedRate = 30000) // Every 30 seconds
    public void logPartitionStats() {
        log.info("📊 Total messages sent: {} | Distribution across partitions tracked", 
                messagesSentCounter.count());
    }
}
