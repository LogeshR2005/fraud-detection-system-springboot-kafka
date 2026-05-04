# 🔍 Fraud Detection System

Real-time financial fraud detection using **Apache Kafka Streams** and **Spring Boot**.

Transactions are streamed through Kafka, and any transaction exceeding a threshold is automatically flagged and routed to a fraud alerts topic — all in real time.

---

## 🏗️ Architecture

```
POST /api/transaction
        │
        ▼
TransactionController
(Generates 50 transactions, amount range ₹8000–₹11000)
        │
        ▼
Kafka Topic: "transcation"
        │
        ▼
FraudDetectionStream (Kafka Streams)
  amount > ₹10,000? ──► YES ──► "fraud-alerts" topic + WARN log
                    └── NO  ──► Passes through
```

---

## 🛠️ Tech Stack

| Technology | Purpose |
|---|---|
| Spring Boot | Application framework |
| Apache Kafka | Message streaming |
| Kafka Streams | Real-time stream processing |
| Jackson | JSON serialization |
| Lombok | Boilerplate reduction |

---

## 📁 Project Structure

```
src/
├── config/
│   └── KafkaConfig.java          # Topic definitions
├── controller/
│   └── TransactionController.java # REST endpoint to trigger transactions
├── events/
│   └── Transaction.java           # Transaction record (id, userId, amount, timestamp)
└── streams/
    └── FraudDetectionStream.java  # Kafka Streams fraud filter logic
```

---

## ⚙️ Setup & Run

### Prerequisites
- Java 21
- Spring Boot 
- Apache Kafka 
- Maven

---

### 1. Start Kafka (KRaft Mode)

```bash
cd /path/to/kafka

# Generate UUID (first time only)
./bin/kafka-storage.sh random-uuid

# Format storage (first time only)
./bin/kafka-storage.sh format -t <UUID> -c config/kraft/server.properties --standalone

# Start Kafka
./bin/kafka-server-start.sh config/kraft/server.properties
```

---

### 2. Run the Application

```bash
./mvnw spring-boot:run
```

---

### 3. Trigger Transactions

```bash
curl -X POST http://localhost:8080/api/transaction
```

This sends **50 random transactions** to Kafka with amounts between ₹8,000–₹11,000.

---

### 4. Monitor Fraud Alerts

```bash
cd /path/to/kafka
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic fraud-alerts --from-beginning
```

---

## 🔍 Fraud Detection Logic

A transaction is flagged as **fraudulent** if:

```java
transaction.amount() > 10000
```

Flagged transactions are:
- Logged as `WARN` in application logs
- Routed to the `fraud-alerts` Kafka topic

---

## 📌 Kafka Topics

| Topic | Purpose |
|---|---|
| `transcation` | All incoming transactions |
| `fraud-alerts` | Flagged fraudulent transactions |

---

## 📝 application.yaml

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
    streams:
      application-id: fraud-detection-streams
      properties:
        default.key.serde: org.apache.kafka.common.serialization.Serdes$StringSerde
        default.value.serde: org.apache.kafka.common.serialization.Serdes$StringSerde
```

---

## 👨‍💻 Author

**Logesh R**  
[GitHub](https://github.com/LogeshR2005)
