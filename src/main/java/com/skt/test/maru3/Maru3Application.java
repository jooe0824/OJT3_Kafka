package com.skt.test.maru3;

import com.skt.test.maru3.Consumer.MyKafkaConsumer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

/**
 * Kafka to Opensearch main class
 */

@SpringBootApplication
public class Maru3Application {

    public static void main(String[] args) throws IOException {
        SpringApplication.run(Maru3Application.class, args);

        String kafkaserveraddress = "13.209.51.15:9094";
        String topic = "adLog";

        MyKafkaConsumer myKafkaConsumer = new MyKafkaConsumer(kafkaserveraddress, topic);
        myKafkaConsumer.startPolling();

    }

}
