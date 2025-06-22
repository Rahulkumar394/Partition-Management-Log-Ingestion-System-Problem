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
        log.info("✅ Initializing Kafka Producer Metrics...");

        messagesSentCounter = Counter.builder("kafka.producer.messages.sent")
                                     .description("Total number of messages sent successfully")
                                     .register(meterRegistry);

        messagesFailedCounter = Counter.builder("kafka.producer.messages.failed")
                                       .description("Total number of failed messages")
                                       .register(meterRegistry);

        sendTimer = Timer.builder("kafka.producer.send.duration")
                         .description("Time taken to send messages to Kafka")
                         .register(meterRegistry);
    }

    @Scheduled(fixedRate = 1000) // 1 second
    public void produceLog() {
        Timer.Sample sample = Timer.start(meterRegistry);

        try {
            String appName = "app-" + String.format("%03d", random.nextInt(100)); // app-000 to app-099
            String logMessage = createLogMessage(appName);

            kafkaTemplate.send(TOPIC, appName, logMessage).whenComplete((result, ex) -> {
                sample.stop(sendTimer); // Stop timing here

                if (ex == null) {
                    messagesSentCounter.increment();
                    int partition = result.getRecordMetadata().partition();
                    log.info("✅ [Sent] Partition: {} | Key: {} | Message: {}", partition, appName, logMessage);
                    partitionDistribution.incrementAndGet();
                } else {
                    messagesFailedCounter.increment();
                    log.error("❌ [Failed] Key: {} | Error: {}", appName, ex.getMessage());
                }
            });

        } catch (Exception e) {
            sample.stop(sendTimer);
            messagesFailedCounter.increment();
            log.error("❌ [Exception] Error producing log", e);
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

    @Scheduled(fixedRate = 30000) // Every 30 seconds
    public void logPartitionStats() {
        log.info("📊 Kafka Producer Metrics >> Total messages sent: {}",
                 messagesSentCounter.count());
    }
}
