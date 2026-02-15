# Spring Boot Kafka Tutorial

A comprehensive tutorial project demonstrating Apache Kafka Producer and Consumer implementation with Spring Boot. This project showcases both string message and JSON object messaging patterns.

## Overview

This Spring Boot application provides a complete setup for Kafka integration using **KRaft mode** (Kafka Raft), which eliminates the need for Zookeeper. The application includes:
- **Kafka Producer**: Send messages to Kafka topics (both string and JSON messages)
- **Kafka Consumer**: Listen and consume messages from Kafka topics
- **REST APIs**: Publish messages via HTTP endpoints
- **Topic Configuration**: Automatic topic creation and configuration
- **KRaft Mode**: Modern, self-managed Kafka without external Zookeeper dependency

## Project Structure

```
springboot-kafka-tutorial/
├── src/main/java/com/kafka/springboot/
│   ├── SpringbootKafkaTutorialApplication.java    # Main Spring Boot Application
│   ├── config/
│   │   └── KafkaTopicConfig.java                  # Kafka Topic Configuration
│   ├── constants/
│   │   └── AppConstants.java                       # Application Constants
│   ├── controller/
│   │   ├── MessageController.java                  # String Message REST Endpoint
│   │   └── JsonMessageController.java              # JSON Message REST Endpoint
│   ├── kafka/
│   │   ├── KafkaProducer.java                      # String Message Producer
│   │   ├── JsonKafkaProducer.java                  # JSON Message Producer
│   │   └── KafkaConsumer.java                      # Kafka Consumer (for both types)
│   └── payload/
│       └── User.java                               # User DTO for JSON Messages
├── src/main/resources/
│   └── application.yaml                            # Application Configuration
└── pom.xml                                         # Maven Configuration
```

## Prerequisites

- **Java 25** or higher
- **Apache Kafka** (3.3+) in KRaft mode (running on localhost:9092)
- **Maven** 3.6 or higher
- **No Zookeeper required** - KRaft (Kafka Raft) replaces Zookeeper

## Technology Stack

- **Spring Boot**: 4.0.2
- **Spring Kafka**: Latest (included with Spring Boot Starter Kafka)
- **Lombok**: For reducing boilerplate code
- **Maven**: Build tool

## Kafka Topics

The application uses two Kafka topics:

| Topic Name | Description |
|------------|-------------|
| `kafka-tutorial` | For JSON message publishing and consuming |
| `kafka-tutorial-string` | For string message publishing and consuming |

## Installation & Setup

### 1. Install Apache Kafka (3.3+)

Download and install Apache Kafka 3.3 or higher from [kafka.apache.org](https://kafka.apache.org/downloads)

### 2. Generate Cluster UUID for KRaft Mode

```bash
# Windows
.\bin\windows\kafka-storage.bat random-uuid

# Linux/Mac
./bin/kafka-storage.sh random-uuid
```

Save the generated UUID for the next step.

### 3. Format Storage Directories for KRaft Mode

```bash
# Windows (replace <UUID> with the UUID from step 2)
.\bin\windows\kafka-storage.bat format -t <UUID> -c .\config\kraft\server.properties

# Linux/Mac
./bin/kafka-storage.sh format -t <UUID> -c ./config/kraft/server.properties
```

### 4. Start Kafka Server in KRaft Mode

```bash
# Windows
.\bin\windows\kafka-server-start.bat .\config\kraft\server.properties

# Linux/Mac
./bin/kafka-server-start.sh ./config/kraft/server.properties
```

**Note**: Zookeeper is not required. Kafka runs in KRaft (Kafka Raft) mode, which is the modern, self-managed approach.

### 5. Clone and Build the Project

```bash
cd springboot-kafka-tutorial
mvn clean install
```

### 6. Run the Application

```bash
mvn spring-boot:run
```

The application will start on `http://localhost:8080`

## API Endpoints

### 1. Publish String Message

**Endpoint**: `GET /api/v1/kafka/publish`

**Query Parameter**:
- `message` (String): The message to publish

**Example**:
```bash
curl "http://localhost:8080/api/v1/kafka/publish?message=Hello%20Kafka"
```

**Response**:
```json
"Message published"
```

### 2. Publish JSON Message

**Endpoint**: `POST /api/v1/kafka/publish`

**Request Body**:
```json
{
  "id": 1,
  "firstName": "John",
  "lastName": "Doe"
}
```

**Example**:
```bash
curl -X POST "http://localhost:8080/api/v1/kafka/publish" \
  -H "Content-Type: application/json" \
  -d '{"id":1,"firstName":"John","lastName":"Doe"}'
```

**Response**:
```json
"JSON message sent to Kafka Topic"
```

## Configuration

The Kafka configuration is defined in `application.yaml`:

```yaml
spring:
  kafka:
    # Consumer Configuration
    consumer:
      bootstrap-servers: localhost:9092
      group-id: myConsumerGroup
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JacksonJsonDeserializer
      properties:
        spring:
          json:
            trusted:
              packages: "com.kafka.springboot.payload"
    
    # Producer Configuration
    producer:
      bootstrap-servers: localhost:9092
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JacksonJsonSerializer
```

### Key Configuration Details

- **Bootstrap Servers**: Kafka broker address (localhost:9092)
- **Consumer Group**: myConsumerGroup - group of consumers reading from topics
- **Auto Offset Reset**: earliest - start reading from the beginning if no offset found
- **Deserializers/Serializers**: Jackson-based for JSON message handling
- **Trusted Packages**: Security setting for JSON deserialization

## Core Components

### 1. KafkaProducer (String Messages)

Sends plain string messages to the `kafka-tutorial-string` topic.

```java
kafkaProducer.sendMessage("Hello World");
```

### 2. JsonKafkaProducer (JSON Messages)

Sends User objects as JSON to the `kafka-tutorial` topic.

```java
User user = new User();
user.setId(1);
user.setFirstName("John");
user.setLastName("Doe");
jsonKafkaProducer.sendJsonMessage(user);
```

### 3. KafkaConsumer

Consumes messages from both topics:
- Listens on `kafka-tutorial` for JSON messages
- Listens on `kafka-tutorial-string` for string messages
- Logs all received messages

## Monitoring

### View Kafka Topics

```bash
# List all topics
kafka-topics --bootstrap-server localhost:9092 --list

# Describe a topic
kafka-topics --bootstrap-server localhost:9092 --topic kafka-tutorial --describe
```

### Monitor Messages

```bash
# Listen to kafka-tutorial topic
kafka-console-consumer --bootstrap-server localhost:9092 --topic kafka-tutorial --from-beginning

# Listen to kafka-tutorial-string topic
kafka-console-consumer --bootstrap-server localhost:9092 --topic kafka-tutorial-string --from-beginning
```

## Testing

Run the test suite:

```bash
mvn test
```

## Key Features

✅ String message producer and consumer  
✅ JSON object producer and consumer  
✅ REST API for message publishing  
✅ Automatic topic configuration  
✅ Consumer group management  
✅ Logging and monitoring  
✅ Jackson-based JSON serialization/deserialization  

## Troubleshooting

### Issue: Cannot connect to Kafka broker

**Solution**: 
1. Ensure Kafka is running on localhost:9092
2. Verify Kafka is started in KRaft mode (Zookeeper is not required)
3. Check that the broker is properly formatted with `kafka-storage.sh format` command

### Issue: Kafka fails to start with KRaft

**Solution**: 
1. Generate a new Cluster UUID using `kafka-storage.sh random-uuid`
2. Format the storage with `kafka-storage.sh format -t <UUID> -c config/kraft/server.properties`
3. Ensure the `data/kraft-combined-logs` directory is writable

### Issue: Consumer not receiving messages

**Solution**: 
1. Check if topics exist
2. Verify consumer group configuration
3. Check application logs for errors

### Issue: JSON deserialization error

**Solution**: Ensure the `trusted.packages` configuration includes `com.kafka.springboot.payload`

## Building JAR

```bash
mvn clean package
```

The executable JAR will be created at:
```
target/springboot-kafka-tutorial-0.0.1-SNAPSHOT.jar
```

Run the JAR:
```bash
java -jar target/springboot-kafka-tutorial-0.0.1-SNAPSHOT.jar
```

## Next Steps

- Add error handling and retry logic
- Implement message partitioning strategies
- Add authentication and security
- Implement message filtering and transformations
- Add metrics and monitoring (Prometheus, Grafana)
- Scale to multi-partition topics

## License

This project is open source and available under the MIT License.

## Author

Kafka Tutorial Project - Spring Boot Integration Demo

## Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Spring Kafka Documentation](https://spring.io/projects/spring-kafka)
- [Spring Boot Documentation](https://spring.io/projects/spring-boot)





