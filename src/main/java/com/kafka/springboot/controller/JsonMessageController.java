package com.kafka.springboot.controller;

import com.kafka.springboot.kafka.JsonKafkaProducer;
import com.kafka.springboot.payload.User;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/kafka")
@RequiredArgsConstructor
public class JsonMessageController {

    private final JsonKafkaProducer jsonKafkaProducer;

    @PostMapping("/publish")
    public ResponseEntity<String> sendMessage(@RequestBody User user) {
        jsonKafkaProducer.sendJsonMessage(user);
        return ResponseEntity.ok().body("JSON message sent to Kafka Topic");
    }


}
