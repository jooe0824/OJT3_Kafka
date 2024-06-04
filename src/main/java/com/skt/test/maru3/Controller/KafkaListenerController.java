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


@Slf4j
@Controller
public class KafkaListenerController {

    @Autowired
    private KafkatoOpensearch kafkatoopensearch;

    /**
     * KafkaListener Consumer
     * @param record
     */
    @KafkaListener(id = "KafkaListenerConsumer", topics="adLog", groupId="my-group")
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


}
