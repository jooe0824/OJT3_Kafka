package com.skt.test.maru3.Controller;


import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.skt.test.maru3.Service.KafkatoOpensearch;
import com.skt.test.maru3.data.KafkaData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.stereotype.Controller;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Controller
public class DataController {

    @Autowired
    private KafkatoOpensearch kafkatoopensearch;

    @KafkaListener(id = "KafkaListenerConsumer", topics="adLog", groupId="my-group")
    public void consumer(List<Object> records) {

//                String recordString = record.toString();
//                log.debug("Received Record : {}", recordString);
//                String jsonStr2 = extractPayload(recordString);
//            log.debug("Received PayloadRecord : {}", records.toString());

//                if (isJsonObject(jsonStr2)) {
            Gson gson = new Gson();
            try {
                for (Object record : records) {


                    //record.toString() json구조의 데이터를 gson을 이용하여 kafkaData 클래스 객체로 변환
                    KafkaData kafkaConsumerData = gson.fromJson(record.toString(), KafkaData.class);
                    log.debug(record.toString());
                    // 2. 여기에 opensearch service로 1번에서 만든 class를 save하면 댐
                    kafkatoopensearch.save(kafkaConsumerData);

                    // ex) opensearchService.save(kafkaConsumerData);
                    log.debug(record.toString());
//                }
//            }catch (JsonSyntaxException e) {
//                       log.error("Error parsing Json: {} - {}", record, e.getMessage());
//                log.error(e.getMessage());
//                    }
//               }else{
//                    log.error("Received JSON is not an object : {}", jsonStr2);
//                    }
//        }
//
            }
        }
        catch (Exception e){
            log.error(e.getMessage());
        }
    }

    private boolean isJsonObject(String jsonString) {
        try {
            JsonObject jsonObject = JsonParser.parseString(jsonString).getAsJsonObject();
            return true;
        } catch (IllegalStateException | JsonSyntaxException e) {
            return false;
        }
    }

    private String extractPayload(String recordString) {
        try {
            JsonObject jsonObject = JsonParser.parseString(recordString).getAsJsonObject();
            return jsonObject.get("payload").toString();
        } catch (Exception e) {
            log.error("Error extracting payload from record: {}", recordString, e);
            return null;
        }
    }



}
