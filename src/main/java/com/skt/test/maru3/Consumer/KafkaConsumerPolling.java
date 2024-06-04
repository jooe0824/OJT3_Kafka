package com.skt.test.maru3.Consumer;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;


/**
 *
 * Consumer Polling Consumer
 */

@Slf4j
public class KafkaConsumerPolling {

    private final KafkaConsumer<String, String> CONSUMER;
    private OpenSearchClientPolling OPENSEARCHCLIENT;

    //OpenSearch에 생성될 인덱스 이름
    String indexName = "maru2";
    //OpenSearch 인덱스 템플릿 이름
    String templateName = "maru2-template";
    //index_patterns : 이 패턴에 맞는 모든 인덱스가 이 템플릿의 설정을 상속받음. shards : 인덱스 생성될 때 샤드의 수
    String templateSource = "{ \"index_patterns\": [\"maru*\"], \"settings\": { \"number_of_shards\": 2 }, \"mappings\": { \"properties\": { \"timestamp\": { \"type\": \"date\" } } } }";
    //index 수명 주기 관리 정책의 이름
    String policyName = "maru2-policy";
    //maru3-policy 정책을 JSON 형식으로 설정
    String policySource = "{ \"policy\": { \"phases\": { \"hot\": { \"actions\": { \"rollover\": { \"max_age\": \"30d\" } } } } } }";


    /**
     * Kafka Consumer Create
     * @param kafkaServerAddress
     * @param topic
     */
    public KafkaConsumerPolling(String kafkaServerAddress, String topic) {

        //properties 객체 생성
        Properties props = new Properties();
        //kafka 브로커가 실행되고 있는 서버의 IP 주소와 포트 번호
        props.put("bootstrap.servers", kafkaServerAddress);
        //컨슈머 그룹 ID 설정
        props.put("group.id", "my-group");
        //Auto Commit
        props.put("enable.auto.commit", "true");
        //가장 오래된 메시지 오프셋부터 read
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //CONSUMER 객체가 topic 을 구독하도록 설정. 구독 설정 후 소비자는 해당 토픽에서 메시지 consume 가능
        //멤버 변수인 CONSUMER에 직접 인스턴스를 달아준 것
        this.CONSUMER = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
        this.CONSUMER.subscribe(Collections.singletonList(topic));

        //Opensearch Client 설정
        this.OPENSEARCHCLIENT = new OpenSearchClientPolling();
        this.OPENSEARCHCLIENT.createIndex(indexName);
        this.OPENSEARCHCLIENT.putTemplate(templateName, templateSource);
        this.OPENSEARCHCLIENT.putPolicy(policyName, policySource);

    }


    /**
     * Kafka consumer polling
     */
    public void startPolling() {
        try {
            Gson gson = new Gson();
            while (true) {
                //컨슈머는 폴링하는 것을 계속 유지하고 타임아웃 주기는 3000ms. 해당 시간만큼 blocking
                ConsumerRecords<String, String> records = this.CONSUMER.poll(Duration.ofMillis(3000));

                List<String> kafkaDataList = new ArrayList<>();
                //poll()은 레코드 전체를 리턴하고, 하나의 메시지만 가져오는 것이 아니므로 반복문 처리
                //각 ConsumeRecord는 개별 Kafka 메시지를 나타냄
                for (ConsumerRecord<String, String> record : records) {
                    log.debug("Consumer - Topic: {}, Partition: {}}, Offset: {}}, Key: %s, Value: %s%n", record.topic(), record.partition(), record.offset(), record.key(), record.value());

                    //각 메시지의 값을 리스트에 추가
                    kafkaDataList.add(record.value());
                    log.debug(record.toString());
                }
                if(!kafkaDataList.isEmpty()) {
                        this.OPENSEARCHCLIENT.bulkInsert(indexName, kafkaDataList);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }


    /**
     * destroy
     */
    public void close() {
        this.CONSUMER.close();
        this.OPENSEARCHCLIENT.close();
    }
}


