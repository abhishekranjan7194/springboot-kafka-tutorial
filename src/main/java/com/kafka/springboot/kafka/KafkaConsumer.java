package com.kafka.springboot.kafka;

import com.kafka.springboot.payload.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import static com.kafka.springboot.constants.AppConstants.JSON_MSG_TOPIC_NAME;
import static com.kafka.springboot.constants.AppConstants.STRING_MSG_TOPIC_NAME;

@Service
public class KafkaConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);


    // Read String Message from Kafka Topic
    @KafkaListener(topics = STRING_MSG_TOPIC_NAME, groupId = "myConsumerGroup")
    public void consume(String message) {
        LOGGER.info("Received message from topic: {}", message);
    }

    // Read JSON Message from Kafka Topic
    @KafkaListener(topics = JSON_MSG_TOPIC_NAME, groupId = "myConsumerGroup")
    public void consumeJsonMessage(User user) {
        LOGGER.info("Received JSON message from the Kafka topic: {}", user.toString());
    }
}
