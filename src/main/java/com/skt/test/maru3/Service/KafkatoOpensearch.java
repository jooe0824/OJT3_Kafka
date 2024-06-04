package com.skt.test.maru3.Service;


import com.google.gson.Gson;
import com.skt.test.maru3.data.KafkaData;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class KafkatoOpensearch {

    private final RestHighLevelClient client;
    private final String indexName = "maru2";

    /**
     * KafkatoOpensearch class에서 RestHighLevelClient Bean 주입받기 위해 @Autowired 사용, 생성자 주입방식 이용
     * @param client
     */
    @Autowired
    public KafkatoOpensearch(RestHighLevelClient client){
        this.client = client;
    }

    public void save(KafkaData kafkaData) throws IOException {

        try {
            Gson gson = new Gson();
            String jsonkafkaData = gson.toJson(kafkaData);
            List<String> kafkaDataList = new ArrayList<>();
            kafkaDataList.add(jsonkafkaData);

            for (String message : kafkaDataList) {
                IndexRequest request = new IndexRequest(indexName)
                        .source(message, XContentType.JSON);
                log.info("Document inserted before response : {}, {}", indexName, request.toString());
                IndexResponse response = client.index(request, RequestOptions.DEFAULT);
                log.info("Document inserted : {}, {}", indexName, response.toString());
            }
        } catch (IOException e) {
            log.error("Error inserting data to OpenSearch: {}", e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error: {}", e.getMessage(), e);
        }


        /**
         * IndexRequest().source()
         * 의 파라미터로 serialized 하지 않은 객체를 넣을 경우,
         * toString() 이 먹혀서, openSearch 에서 필드를 구분하지 못하고 통 String으로 들어가게 됨. (아래 코드)
         * serialize 해서 json string 으로 넣어야지
         * openSearch 에서 필드를 구분할 수 있었음 (위 코드)
         */
//        IndexRequest request = new IndexRequest(indexName)
//                .source(kafkaData, XContentType.JSON);

    }
}
