package com.skt.test.maru3.Service;


import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
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

    /**
     * Kafka to Opensearch : Data Save
     * @param list
     * @throws IOException
     */
    public void save(List<String> list) throws IOException {
        //여러 개의 인덱스 요청을 모아 일괄 요청으로 처리하기 위함
        BulkRequest bulkRequest = new BulkRequest();

        //list로 넘어온각 문자열 jsonData String에 대해 반복
        for (String jsonData : list) {
            //jsonData Str 을 Opensearch에 인덱싱할 수 있도록 IndexRequest 객체로 변환
            //XContentType.JSON => 데이터가 JSON 형식임을 나타냄. 즉, JSON 형식인 jsonData string을 IndexRequest 객체로 변환.
            IndexRequest request = new IndexRequest(indexName).source(jsonData, XContentType.JSON);
            //BulkRequest 객체에 IndexRequest 객체 추가.
            bulkRequest.add(request);
        }

        //BulkRequest를 Opensearch client 통해 실행하고 그 결과를 BulkResponse 객체에 저장
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

        if (bulkResponse.hasFailures()) {
           log.error("Bulk insert had failures: " + bulkResponse.buildFailureMessage());
        } else {
            log.info("Bulk insert completed successfully.");
        }
    }
}
