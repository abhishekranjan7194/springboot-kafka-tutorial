package com.kafka.springboot.kafka;

import com.kafka.springboot.payload.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import static com.kafka.springboot.constants.AppConstants.JSON_MSG_TOPIC_NAME;

@Service
@Slf4j
public class JsonKafkaProducer {

    private final KafkaTemplate<String, User> kafkaTemplate;

    public JsonKafkaProducer(KafkaTemplate<String, User> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendJsonMessage(User user) {
        log.info("Message Sent to the Kafka Topic: {}", user.toString());
        Message<User> message = MessageBuilder.withPayload(user).setHeader(KafkaHeaders.TOPIC, JSON_MSG_TOPIC_NAME).build();
        kafkaTemplate.send(message);
    }
}
