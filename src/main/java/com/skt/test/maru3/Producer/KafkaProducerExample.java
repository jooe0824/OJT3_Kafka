package com.skt.test.maru3.Producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


import java.util.Properties;

public class KafkaProducerExample {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        //Kafka 클러스터의 주소 지정
        //props.put("bootstrap.servers", "peter-kafka01.foo.bar:9092, peter-kafka02.foo.bar:9092");
        //브로커 리스트 정의
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //msg key와 value는 문자열타입이므로 카프카의 기본 StringSerializer 지정
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        //properties 객체를 전달하여 새 프로듀서 생성


        try {
            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", "key" + i, "value" + i);
                //producerRecord 객체 생성
                producer.send(record);
                //send() 메소드를 사용하여 메시지 전송 후 자바 Future 객체로 RecordMetadata return
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
