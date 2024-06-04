package com.skt.test.maru3;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.io.IOException;

/**
 * Kafka to Opensearch main class
 */

@SpringBootApplication
@EnableScheduling
public class Maru3Application {

    public static void main(String[] args) throws IOException {
        SpringApplication.run(Maru3Application.class, args);

    }

}
