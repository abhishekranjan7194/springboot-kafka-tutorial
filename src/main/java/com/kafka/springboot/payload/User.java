package com.kafka.springboot.payload;

import lombok.*;

@Getter
@Setter
@ToString
public class User {
    private int id;
    private String firstName;
    private String lastName;
}
