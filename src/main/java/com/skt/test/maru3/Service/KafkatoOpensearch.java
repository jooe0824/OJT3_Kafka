package com.skt.test.maru3.Service;


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

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkatoOpensearch {

    private final RestHighLevelClient client;

    public void save(KafkaData kafkaData) {
        String indexName = "maru3";

        Gson gson = new Gson();
        String message = gson.toJson(kafkaData);

        IndexRequest request = new IndexRequest(indexName)
                .source(message, XContentType.JSON);
        try {
            //IndexRequest 객체를 Opensearch에 전송, 응답을 IndexResponse 객체에 저장
            IndexResponse response = client.index(request, RequestOptions.DEFAULT);
            log.info("Document inserted : {}", response.getId());
        } catch (IOException e) {
            log.error("Error during bulk insert: {}", e.getMessage());
        }
    }




}
