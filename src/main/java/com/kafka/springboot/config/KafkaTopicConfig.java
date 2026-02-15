package com.kafka.springboot.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import static com.kafka.springboot.constants.AppConstants.JSON_MSG_TOPIC_NAME;
import static com.kafka.springboot.constants.AppConstants.STRING_MSG_TOPIC_NAME;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic kafkaTutorialJsonMsgTopic() {
        return TopicBuilder.name(JSON_MSG_TOPIC_NAME).build();
    }

    @Bean
    public NewTopic kafkaTutorialStringMsgTopic() {
        return TopicBuilder.name(STRING_MSG_TOPIC_NAME).build();
    }
}
