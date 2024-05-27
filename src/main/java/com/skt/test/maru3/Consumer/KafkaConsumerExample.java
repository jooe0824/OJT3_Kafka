package com.skt.test.maru3.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.RestClient;
import org.opensearch.common.xcontent.XContentType;
import org.apache.http.HttpHost;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerExample {
    //public static void run(){

    public static void main(String[] args) {
        //properties 객체 생성
        Properties props = new Properties();
        //kafka 브로커가 실행되고 있는 서버의 IP 주소와 포트 번호
        props.put("bootstrap.servers", "13.209.51.15:9094");
        //컨슈머 그룹 ID 설정
        //props.put("group.id", "my-group");
        //Auto Commit
        //props.put("enable.auto.commit", "true");
        //props.put("auto.offset.reset", "latest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //consumer.subscribe(Arrays.asList("adLog"));
        consumer.subscribe(Collections.singletonList("adLog"));

        /*
        // OpenSearch client setup
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("13.209.51.15", 9200, "http"))
        );*/

        try{
            while (true) {
                //컨슈머는 폴링하는 것을 계속 유지하고 타임아웃 주기는 100ms. 해당 시간만큼 blocking
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                //records.forEach(record -> System.out.printf("Offset: %d, Key: %s, Value: %s%n", record.offset(), record.key(), record.value()));

                //poll()은 레코드 전체를 리턴하고, 하나의 메시지만 가져오는 것이 아니므로 반복문 처리
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Topic: %s, Partition: %s, Offset: %d, Key: %s, Value: %s%n", record.topic(), record.partition(), record.offset(), record.key(), record.value());

                    /*
                    // Create OpenSearch IndexRequest, OpenSearch로 메시지 전송
                    IndexRequest indexRequest = new IndexRequest("telegraf-metrics")
                            //.id(String.valueOf(record.offset()))
                            .source(record.value(), XContentType.JSON);

                    try {// Send data to OpenSearch
                        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                        System.out.printf("Indexed document with id %s%n", indexResponse.getId());
                    }catch (IOException e) {
                        System.err.printf("Failed to index doc : %s%n", e.getMessage());
                        e.printStackTrace();
                    }*/
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally{
            /*try{
                openSearchClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }*/
            consumer.close();
        }
    }
}
