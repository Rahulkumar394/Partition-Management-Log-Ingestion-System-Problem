To test the **Kafka log producer and consumers** running in Docker on your **Mac**, you can use the following approaches:

---
### ✅ **Step 0: Running the command**

```bash
docker-compose up --build
```

## ✅ 1. **Verify Docker Containers are Running**

You're already running this:

```bash
docker ps
```

Make sure you see:

* `log-ingestion-system-log-producer`
* `log-ingestion-system-log-consumer-1` (and 2, 3)
* `cp-kafka`, `cp-zookeeper`
* `kafka-exporter`, `prometheus`, `grafana`

---

## ✅ 2. **Exec into the Kafka container**

Kafka is running inside the container `log-ingestion-system-kafka-1`.

### 🔹 Open a shell in the Kafka container:

```bash
docker exec -it log-ingestion-system-kafka-1 bash
```

Once inside:

---

## ✅ 3. **Verify Topic Exists**

```bash
kafka-topics --bootstrap-server localhost:9092 --list
```

You should see:

```
logs.events
```

---

## ✅ 4. **Check Topic Details**

```bash
kafka-topics --bootstrap-server localhost:9092 --describe --topic logs.events
```

This should show 50 partitions and leader/replica info.

---

## ✅ 5. **Consume Messages Manually (Test Only)**

To directly consume messages from the topic:

```bash
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic logs.events \
  --from-beginning \
  --property print.key=true \
  --property print.partition=true
```

You should see messages like:

```
app-002	[INFO] UserLogin - UserLogin executed at 2025-06-22T08:45:12.123Z by app-002	partition: 21
```

---

## ✅ 6. **Test Producer from Browser or REST Tool**

If your producer is exposing Prometheus metrics or any actuator endpoint at:

```
http://localhost:8082/actuator
http://localhost:8082/actuator/prometheus
```

Open this URL in browser or use:

```bash
curl http://localhost:8082/actuator
```

You can also check producer logs:

```bash
docker logs log-ingestion-system-log-producer-1 -f
```

You should see:

```
✅ Sent to partition 12 | Key: app-047 | Message: [INFO] CacheUpdate - CacheUpdate executed at...
```

---

## ✅ 7. **Check Consumers Are Receiving Logs**

Check logs for each consumer:

```bash
docker logs log-ingestion-system-log-consumer-1-1 -f
docker logs log-ingestion-system-log-consumer-2-1 -f
docker logs log-ingestion-system-log-consumer-3-1 -f
```

They should print lines like:

```
Consumer[consumer-1] - Partition: 23 | Offset: 1021 | Key: app-045 | Count: 200 | Message: ...
```

---

## ✅ 8. **Verify Prometheus and Grafana Metrics**

### Prometheus

Open in browser:

```
http://localhost:9090
```

Query metrics:

* `kafka_producer_messages_sent_total`
* `kafka_consumer_lag`
* `kafka_consumer_records_consumed_total`

### Grafana

Open:

```
http://localhost:3000
Login: admin / admin
```

Add a data source (Prometheus) pointing to `http://prometheus:9090` and import dashboards or build your own.

---

## ✅ 9. **Producer Profiling via Actuator Metrics**

You can also test with:

```bash
curl http://localhost:8081/actuator/metrics/kafka.producer.messages.sent
curl http://localhost:8081/actuator/metrics/kafka.producer.send.duration
```

---

## ⚠️ If Logs Aren't Coming In

* Check `SPRING_PROFILES_ACTIVE=producer` is set in the `log-producer`
* Check partitions are created with `--describe`
* Check `bootstrap-servers` is `kafka:9092` (inside Docker) or `localhost:29092` (from host)

---

Here’s a **custom Grafana dashboard JSON** you can import directly to monitor your **Kafka Producer, Consumer, and Lag metrics** using **Micrometer + Prometheus**.

---

### ✅ How to Import in Grafana

1. Open Grafana: [http://localhost:3000](http://localhost:3000)
2. Login (default: `admin` / `admin`)
3. Left Sidebar → `+` → **Import**
4. Paste JSON below in "Import via panel json" → click **Load**
5. Choose your Prometheus data source → click **Import**

---

### 📊 Dashboard Includes

* 🔄 Producer Message Sent Rate
* 📉 Failed Messages Count
* ⏱ Producer Send Duration (Timer)
* ⌛ Partition-wise Lag (with partition label)
* 🧠 Consumer Processed Messages Count

---

### 📁 Grafana Dashboard JSON (Copy Below)

```json
{
  "annotations": {
    "list": []
  },
  "editable": true,
  "gnetId": null,
  "graphTooltip": 0,
  "id": null,
  "iteration": 1719050102847,
  "panels": [
    {
      "datasource": "Prometheus",
      "fieldConfig": {
        "defaults": {
          "unit": "ops"
        },
        "overrides": []
      },
      "gridPos": {
        "h": 6,
        "w": 8,
        "x": 0,
        "y": 0
      },
      "id": 1,
      "title": "\ud83d\udd04 Messages Sent (Producer)",
      "type": "timeseries",
      "targets": [
        {
          "expr": "rate(kafka_producer_messages_sent_total[1m])",
          "interval": "",
          "legendFormat": "sent",
          "refId": "A"
        }
      ]
    },
    {
      "datasource": "Prometheus",
      "fieldConfig": {
        "defaults": {
          "unit": "none"
        },
        "overrides": []
      },
      "gridPos": {
        "h": 6,
        "w": 8,
        "x": 8,
        "y": 0
      },
      "id": 2,
      "title": "\u274c Messages Failed (Producer)",
      "type": "timeseries",
      "targets": [
        {
          "expr": "rate(kafka_producer_messages_failed_total[1m])",
          "legendFormat": "failed",
          "refId": "A"
        }
      ]
    },
    {
      "datasource": "Prometheus",
      "fieldConfig": {
        "defaults": {
          "unit": "ms"
        },
        "overrides": []
      },
      "gridPos": {
        "h": 6,
        "w": 16,
        "x": 0,
        "y": 6
      },
      "id": 3,
      "title": "\u23f1 Send Duration (Producer)",
      "type": "timeseries",
      "targets": [
        {
          "expr": "kafka_producer_send_duration_seconds_sum / kafka_producer_send_duration_seconds_count",
          "legendFormat": "avg send duration",
          "refId": "A"
        }
      ]
    },
    {
      "datasource": "Prometheus",
      "fieldConfig": {
        "defaults": {
          "unit": "short"
        },
        "overrides": []
      },
      "gridPos": {
        "h": 8,
        "w": 24,
        "x": 0,
        "y": 12
      },
      "id": 4,
      "title": "\u231b Kafka Consumer Lag by Partition",
      "type": "bargauge",
      "options": {
        "orientation": "horizontal",
        "displayMode": "gradient"
      },
      "targets": [
        {
          "expr": "kafka_consumer_lag",
          "legendFormat": "{{partition}}",
          "refId": "A"
        }
      ]
    },
    {
      "datasource": "Prometheus",
      "fieldConfig": {
        "defaults": {
          "unit": "ops"
        },
        "overrides": []
      },
      "gridPos": {
        "h": 6,
        "w": 24,
        "x": 0,
        "y": 20
      },
      "id": 5,
      "title": "\ud83e\udde0 Consumer Processed Count",
      "type": "timeseries",
      "targets": [
        {
          "expr": "rate(processed_messages_total[1m])",
          "legendFormat": "processed",
          "refId": "A"
        }
      ]
    }
  ],
  "schemaVersion": 36,
  "tags": [
    "kafka",
    "micrometer",
    "log-ingestion"
  ],
  "templating": {
    "list": []
  },
  "title": "Kafka Log Ingestion Dashboard",
  "version": 1
}
```

---

### ✅ Final Step: Ensure Prometheus Metric Names Match

This dashboard uses the following metric names:

| Metric Name                                           | Description                           |
| ----------------------------------------------------- | ------------------------------------- |
| `kafka_producer_messages_sent_total`                  | Counter for messages sent             |
| `kafka_producer_messages_failed_total`                | Counter for failed sends              |
| `kafka_producer_send_duration_seconds_sum` + `_count` | Timer for send duration               |
| `kafka_consumer_lag`                                  | Lag per partition (custom Gauge)      |
| `processed_messages_total`                            | Custom counter (optional, or replace) |

If your metrics use slightly different names, let me know — I’ll customize the JSON for you.


