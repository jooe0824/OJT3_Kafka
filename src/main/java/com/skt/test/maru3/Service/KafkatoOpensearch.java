package com.skt.test.maru3.Service;


import com.skt.test.maru3.consumer.MyOpenSearchClient2;
import com.skt.test.maru3.data.KafkaData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.springframework.stereotype.Service;
import com.google.gson.Gson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class KafkatoOpensearch {

    private final RestHighLevelClient client;
    private final MyOpenSearchClient2 myOpenSearchClient2;

    String indexName = "maru2";
    String templateName = "maru2-template";
    //index_patterns : 이 패턴에 맞는 모든 인덱스가 이 템플릿의 설정을 상속받음. shards : 인덱스 생성될 때 샤드의 수
    String templateSource = "{ \"index_patterns\": [\"maru2*\"], \"settings\": { \"number_of_shards\": 2 }, \"mappings\": { \"properties\": { \"timestamp\": { \"type\": \"date\" } } } }";
    //index 수명 주기 관리 정책의 이름
    String policyName = "maru2-policy";
    //maru3-policy 정책을 JSON 형식으로 설정
    String policySource = "{ \"policy\": { \"phases\": { \"hot\": { \"actions\": { \"rollover\": { \"max_age\": \"30d\" } } } } } }";

    public KafkatoOpensearch(
            MyOpenSearchClient2 myOpenSearchClient2,
            RestHighLevelClient client
    ){
        this.myOpenSearchClient2 = myOpenSearchClient2;
        this.client = client;
        myOpenSearchClient2.createIndex(indexName);
        myOpenSearchClient2.putTemplate(templateName, templateSource);
        myOpenSearchClient2.putPolicy(policyName, policySource);
    }

    public void save(KafkaData kafkaData) {

        Gson gson = new Gson();
        String jsonifiedKafkaData = gson.toJson(kafkaData);
        List<String> kafkaDataList = new ArrayList<>();
        kafkaDataList.add(jsonifiedKafkaData);
        myOpenSearchClient2.bulkInsert(indexName, kafkaDataList);

        System.out.println("inserting " + kafkaData.toString());

        /**
         * IndexRequest().source()
         * 의 파라미터로 serialized 하지 않은 객체를 넣을 경우,
         * toString() 이 먹혀서, openSearch 에서 필드를 구분하지 못함 (아래 코드)
         * serialize 해서 json string 으로 넣어야지
         * openSearch 에서 필드를 구분할 수 있었음 (위 코드)
         */

//        IndexRequest request = new IndexRequest(indexName)
//                .source(kafkaData, XContentType.JSON);
//        try {
//            //IndexRequest 객체를 Opensearch에 전송, 응답을 IndexResponse 객체에 저장
//            client.index(request, RequestOptions.DEFAULT);
////            log.info("Document inserted : {}", response.getId());
//        } catch (IOException e) {
//            log.error("Error during bulk insert: {}", e.getMessage());
//        }
    }




}
