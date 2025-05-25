# Partition-Management-Log-Ingestion-System-Problem
Distribute log ingestion across 50+ partitions using appName as the key.

**Tech Stack:**

- Java 17
- Spring Boot
- Spring Kafka
- Prometheus + Grafana
- Kafka (Docker)

**How It Works:**
Simulated 100 apps produce logs to logs.events using key = appName.
Multiple consumers read in a balanced way.
Rules to Follow:
Kafka-Specific:

Partition by appName
Monitor consumer lag
Set topic retention appropriately
Code Quality:

Producer utility to send random logs
Separate consumers by groupId

**Testing:**

Unit: Producer sends to correct partition
Integration: Verify partition balance
Observability: Prometheus metrics on lag
