package com.log.producer;

import java.time.Instant;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Profile("producer")
public class LogProducer {
	private static final Logger log = LoggerFactory.getLogger(LogProducer.class);
	private static final String TOPIC = "logs.events";
	private final KafkaTemplate<String, String> kafkaTemplate;
	private final Random random = new Random();

	public LogProducer(KafkaTemplate<String, String> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	@Scheduled(fixedRate = 1000)
	public void produceLog() {
		try {
			String appName = "app-" + random.nextInt(100);
			String logMessage = "Log from " + appName + " at " + Instant.now();

			kafkaTemplate.send(TOPIC, appName, logMessage).whenComplete((result, ex) -> {
				if (ex == null) {
					log.info("Produced log for key {} partition {}", appName, result.getRecordMetadata().partition());
				} else {
					log.error("Failed to produce log", ex);
				}
			});

		} catch (Exception e) {
			log.error("Error producing log", e);
		}
	}

}
