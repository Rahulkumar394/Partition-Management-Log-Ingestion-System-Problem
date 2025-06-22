package com.log.service;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

@Service
@Profile("consumer")
public class PartitionMonitoringService {
    
    private static final Logger log = LoggerFactory.getLogger(PartitionMonitoringService.class);
    private static final String TOPIC_NAME = "logs.events";
    private static final String CONSUMER_GROUP = "log-consumer-group";
    
    private final AdminClient adminClient;
    private final MeterRegistry meterRegistry;
    private final Map<Integer, Double> partitionLagMap = new ConcurrentHashMap<>();
    
    @Autowired
    public PartitionMonitoringService(KafkaAdmin kafkaAdmin, MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        Properties props = new Properties();
        props.putAll(kafkaAdmin.getConfigurationProperties());
        this.adminClient = AdminClient.create(props);
        
        for (int i = 0; i < 50; i++) {
            final int partition = i;
            Gauge.builder("kafka.consumer.lag", () -> getPartitionLag(partition))
                .description("Consumer lag for partition")
                .tag("partition", String.valueOf(partition))
                .tag("topic", TOPIC_NAME)
                .tag("group", CONSUMER_GROUP)
                .register(meterRegistry);
        }

    }
    
    @Scheduled(fixedRate = 30000) // Every 30 seconds
    public void monitorConsumerLag() {
        try {
            // Get consumer offsets
            ListConsumerGroupOffsetsResult offsetsResult = adminClient.listConsumerGroupOffsets(CONSUMER_GROUP);
            Map<TopicPartition, OffsetAndMetadata> offsets = offsetsResult.partitionsToOffsetAndMetadata().get();
            
            // Get latest offsets for all partitions
            Map<TopicPartition, OffsetSpec> latestOffsetSpecs = new ConcurrentHashMap<>();
            for (int i = 0; i < 50; i++) {
                TopicPartition tp = new TopicPartition(TOPIC_NAME, i);
                latestOffsetSpecs.put(tp, OffsetSpec.latest());
            }
            
            Map<TopicPartition, org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo> latestOffsets = 
                    adminClient.listOffsets(latestOffsetSpecs).all().get();
            
            // Calculate lag for each partition
            for (int i = 0; i < 50; i++) {
                TopicPartition tp = new TopicPartition(TOPIC_NAME, i);
                
                long consumerOffset = offsets.getOrDefault(tp, new OffsetAndMetadata(0)).offset();
                long latestOffset = latestOffsets.get(tp).offset();
                long lag = Math.max(0, latestOffset - consumerOffset);
                
                partitionLagMap.put(i, (double) lag);
                
                if (lag > 0) {
                    log.info("Partition {} lag: {}, Consumer offset: {}, Latest offset: {}", 
                            i, lag, consumerOffset, latestOffset);
                }
            }
            
        } catch (Exception e) {
            log.error("Error monitoring consumer lag", e);
        }
    }
    
    public Double getPartitionLag(int partition) {
        return partitionLagMap.getOrDefault(partition, 0.0);
    }
    
    @Scheduled(fixedRate = 60000) // Every minute
    public void logPartitionDistribution() {
        try {
            Map<TopicPartition, OffsetAndMetadata> offsets = 
                    adminClient.listConsumerGroupOffsets(CONSUMER_GROUP).partitionsToOffsetAndMetadata().get();
            
            log.info("=== Partition Distribution Summary ===");
            int activePartitions = 0;
            for (int i = 0; i < 50; i++) {
                TopicPartition tp = new TopicPartition(TOPIC_NAME, i);
                long offset = offsets.getOrDefault(tp, new OffsetAndMetadata(0)).offset();
                if (offset > 0) {
                    activePartitions++;
                    log.info("Partition {} has {} messages consumed", i, offset);
                }
            }
            log.info("Total active partitions: {}/50", activePartitions);
            
        } catch (Exception e) {
            log.error("Error logging partition distribution", e);
        }
    }
}