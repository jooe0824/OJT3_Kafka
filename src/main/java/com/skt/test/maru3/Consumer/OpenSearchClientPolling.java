package com.skt.test.maru3.Consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.*;
import org.opensearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

@Slf4j
public class OpenSearchClientPolling {

    //client 객체 = RestHighLevelClient의 인스턴스
    private final RestHighLevelClient client;


    /**
     * OpenSearch Connect
     */
    public OpenSearchClientPolling() {
        //HttpHost 객체 생성. 해당 host, port 서버와 상호작용할 수 있도록 설정.
        RestClientBuilder builder = RestClient.builder(
                new HttpHost("13.209.51.15", 9200, "http"));

        this.client = new RestHighLevelClient(builder);
    }


    /**
     * Create Index
     * @param indexName
     */
    public void createIndex(String indexName) {

        //PUT 메서드를 사용하여 새로운 인덱스 생성
        Request request = new Request("PUT", "/" + indexName);
        try {
            //저수준 클라이언트 활용하여 직접 HTTP 요청
            this.client.getLowLevelClient().performRequest(request);
            log.info("Index created: " + indexName);
        } catch (IOException e) {
            log.error("Error creating index: {}", e.getMessage());
        }
    }


    /**
     * Create Template
     * @param templateName
     * @param templateSource
     */
    public void putTemplate(String templateName, String templateSource) {
        Request request = new Request("PUT", "/_index_template/" + templateName);
        request.setJsonEntity(templateSource);
        try {
            this.client.getLowLevelClient().performRequest(request);
            log.info("Template created: " + templateName);
        } catch (IOException e) {
            log.error("Error putting template: {}", e.getMessage());
        }
    }


    /**
     * Create Policy
     * @param policyName
     * @param policySource
     */
    public void putPolicy(String policyName, String policySource) {
        Request request = new Request("PUT", "/_ilm/policy/" + policyName);
        request.setJsonEntity(policySource);
        try {
            this.client.getLowLevelClient().performRequest(request);
            log.info("Policy created: " + policyName);
        } catch (IOException e) {
            log.error("Error putting policy: {}", e.getMessage());
        }
    }


    /**
     * Insert Data from Kafka to Opensearch
     * @param indexName
     * @param kafkaDataList
     */
    public void bulkInsert(String indexName, List<String> kafkaDataList) throws IOException {
        for (String kafkaData : kafkaDataList) {
            //String kafkaData를 JSON(XContentType.JSON) 타입으로 지정
            IndexRequest request = new IndexRequest(indexName)
                    .source(kafkaData, XContentType.JSON);
            IndexResponse response = this.client.index(request, RequestOptions.DEFAULT);
            log.info("Document inserted : {}", response.getId());
//            try {
//                //IndexRequest 객체를 Opensearch에 전송, 응답을 IndexResponse 객체에 저장
//                IndexResponse response = this.client.index(request, RequestOptions.DEFAULT);
//                log.info("Document inserted : {}", response.getId());
//            } catch (IOException e) {
//                log.error("Error during bulk insert: {}", e.getMessage());
//            }
        }
    }


    /**
     * destroy
     */
    public void close() {
        try {
            this.client.close();
        } catch (IOException e) {
            log.error("Error closing OpenSearch client: {}", e.getMessage());
        }
    }
}

