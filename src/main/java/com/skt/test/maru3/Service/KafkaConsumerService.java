package com.skt.test.maru3.Service;

import com.google.gson.Gson;
import com.skt.test.maru3.data.KafkaData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

@Slf4j
@Service
public class KafkaConsumerService {


    private final Consumer<String, String> kafkaConsumer;


    public KafkaConsumerService(
            Consumer<String, String> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
        this.kafkaConsumer.subscribe(Collections.singletonList("adLog"));
    }
//
//    @Scheduled(fixedDelay = 5000)
//    public void pollMessages() {
//        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(3000));
//        records.forEach(record -> {
//            System.out.println("Received Message: " + record.value());
//        });
//    }

    @KafkaListener(id = "kafkaconsumer", topics="adLog", groupId="my-group")
    public void consumer(List<Object> records){
        try {
            for(Object record : records) {
                Gson gson = new Gson();
                KafkaData kafkaConsumerData = gson.fromJson(record.toString(), KafkaData.class);
                System.out.printf("Subscriber - ", record);

                log.debug(record.toString());
            }
        }
        catch (Exception e){
            log.error(e.getMessage());
        }
    }
}
