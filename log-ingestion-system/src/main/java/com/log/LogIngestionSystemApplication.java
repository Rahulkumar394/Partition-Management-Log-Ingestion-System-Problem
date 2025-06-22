//package com.log;
//
//import org.springframework.boot.SpringApplication;
//import org.springframework.boot.autoconfigure.SpringBootApplication;
//import org.springframework.scheduling.annotation.EnableScheduling;
//
//@SpringBootApplication
//@EnableScheduling
//public class LogIngestionSystemApplication {
//
//	public static void main(String[] args) {
//		SpringApplication.run(LogIngestionSystemApplication.class, args);
//	}
//
//}


package com.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class LogIngestionSystemApplication {
    
    private static final Logger log = LoggerFactory.getLogger(LogIngestionSystemApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(LogIngestionSystemApplication.class, args);
    }
    
    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady(ApplicationReadyEvent event) {
        Environment env = event.getApplicationContext().getEnvironment();
        String[] activeProfiles = env.getActiveProfiles();
        String appName = env.getProperty("spring.application.name", "log-ingestion-system");
        String kafkaServers = env.getProperty("spring.kafka.bootstrap-servers", "not-configured");
        
        log.info("=================================================");
        log.info("Application: {} started successfully!", appName);
        log.info("Active Profiles: {}", String.join(", ", activeProfiles));
        log.info("Kafka Bootstrap Servers: {}", kafkaServers);
        log.info("=================================================");
        
        // Keep the application running
        if (activeProfiles.length > 0 && activeProfiles[0].equals("consumer")) {
            log.info("Consumer application is ready to process messages...");
        }
    }
}