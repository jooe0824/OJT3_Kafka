package com.skt.test.maru3.Controller;


import com.google.gson.Gson;
import com.skt.test.maru3.Service.KafkatoOpensearch;
import com.skt.test.maru3.data.KafkaData;
import lombok.extern.slf4j.Slf4j;
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
     * @param messages
     */
    @KafkaListener(id = "KafkaListenerConsumer", topics = "adLog", groupId = "my-group1")
    //Kafka로부터 받은 메시지들을 String list로 전달받음
    public void listen(List<String> messages) {
        log.debug("messages size: {}", messages.size());

        List<String> kafkaDataList = new ArrayList<>();
        try {
            //메시지 리스트에 있는 String 타입의 각 메시지('record') 순차 처리
            for (String record : messages) {
                //record 값이 null 이 아니고 비어있지 않은 경우에만 내부 로직 수행
                if(record != null && !record.isEmpty()) {
                    //JSON 형태의 jsonString 'record'를 'KafkaData' 타입의 객체로 반환
                    KafkaData kafkaConsumerData = this.GSON.fromJson(record, KafkaData.class);
                    //'KafkaData' 타입의 객체로 처리된 데이터를 다시 json 형태로 변환하여 List에 추가
                    kafkaDataList.add(this.GSON.toJson(kafkaConsumerData));


                    /**
                     * record <-> KafkaData 직렬화,역직렬화 하지 않고
                     * List<String> message에서 읽어온 jsonString record를 바로 KafkaDataList에 add 하는 경우 (아래 코드)
                     */
//                  kafkaDataList.add(record);
                }
            }
            kafkatoopensearch.save(kafkaDataList);
        }catch (Exception e){
            log.error("error: {}",e.getMessage());
        }
    }
}
