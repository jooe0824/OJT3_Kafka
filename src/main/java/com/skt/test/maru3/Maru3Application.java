package com.skt.test.maru3;

import com.skt.test.maru3.Consumer.KafkaConsumerExample;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.io.IOException;


@SpringBootApplication
public class Maru3Application {

    public static void main(String[] args) throws IOException {
        SpringApplication.run(Maru3Application.class, args);
        //SpringApplication.run(KafkaConsumerExample.class, args);
        //KafkaConsumerExample.main(args);
    }

    /*
    @Bean
    public CommandLineRunner run() {
        return args -> {
            KafkaConsumerExample kafkaConsumerExample = new KafkaConsumerExample();
            Thread kafkaConsumerThread = new Thread(() -> {
                try {
                    kafkaConsumerExample.run();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            kafkaConsumerThread.start();
        };
    }*/

}
