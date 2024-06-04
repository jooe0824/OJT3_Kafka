package com.skt.test.maru3.Controller;


import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.skt.test.maru3.Service.KafkatoOpensearch;
import com.skt.test.maru3.data.KafkaData;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Controller;

import java.util.ArrayList;
import java.util.List;


@Slf4j
@Controller
public class KafkaListenerController {

    @Autowired
    private KafkatoOpensearch kafkatoopensearch;

    @Autowired
    private  Gson GSON;
    /**
     * KafkaListener Consumer
     * @param record
     */

   //  @KafkaListener(id = "KafkaListenerConsumer", topics="adLog", groupId="my-group1")
    public void consumer(ConsumerRecord<Object, String> record) {

        try {
            Gson gson = new Gson();

            //record.toString() json구조의 데이터를 gson을 이용하여 kafkaData 클래스 객체로 변환
            KafkaData kafkaConsumerData = gson.fromJson(record.value(), KafkaData.class);
            // 2. opensearch service로 1번에서 만든 class save
            kafkatoopensearch.save(kafkaConsumerData);
        }catch (Exception e){
            log.error(e.getMessage());
        }
    }


    /**
     *
     * author  : goodhyoju
     * date    : 2024-06-04 17:58
     * description :
     * @param messages
     */
    @KafkaListener(id = "KafkaListenerConsumer1", topics = "adLog", groupId = "my-group5")
    public void listen(List<String> messages) {
        log.debug("messages size: {}", messages.size());

        List<String> kafkaDataList = new ArrayList<>();
        try {
            for (String record : messages) {
                if(record != null && !record.isEmpty()) {
                    KafkaData kafkaConsumerData = this.GSON.fromJson(record, KafkaData.class);

                    kafkaDataList.add(this.GSON.toJson(kafkaConsumerData));
                }
            }
            kafkatoopensearch.save(kafkaDataList);
        }catch (Exception e){
            log.error("error: {}",e.getMessage());
       //     log.debug(this.GSON.toJson(kafkaDataList));
        }
    }
}
