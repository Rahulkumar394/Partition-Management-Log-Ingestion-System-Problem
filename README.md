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



================================================
Partition Management — Log Ingestion System
=================================================
Step 1: Start the Entire System
# Saare containers ko build aur background mein start karein
docker-compose up --build -d

# Check karein ki saare containers 'Up' ya 'healthy' state mein hain
docker-compose ps

Step 2: Verify Topic Creation 
docker-compose logs kafka-init

Step 3: Show the System in Action (Producer & Consumers)
# Producer ke logs live dekhein
docker-compose logs -f log-producer

# Teeno consumers ke logs ek saath dekhein
docker-compose logs -f log-consumer-1 log-consumer-2 log-consumer-3

Step 4: Visualize with Grafana (The "Wow" Factor)
Open Grafana: Browser mein http://localhost:3000 kholein.
Username: admin
Password: admin (aapko naya password set karne ko kaha ja sakta hai).
Add Prometheus Data Source:
Left side menu mein gear icon (Configuration) -> Data Sources par click karein.
Add data source -> Prometheus.
URL field mein daalein: http://prometheus:9090.
Neeche Save & Test par click karein. Aapko "Data source is working" ka message aana chahiye.

mport Kafka Dashboard:
Left side menu mein plus icon (Create) -> Import.
"Import via grafana.com" wale box mein dashboard ID 15931 (ek popular Kafka Exporter Dashboard) ya 721 daalein aur Load pe click karein.
Agle page par, neeche Prometheus data source dropdown mein "Prometheus" select karein aur Import pe click karein.

