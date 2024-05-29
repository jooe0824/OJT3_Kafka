//package com.skt.test.maru3.Consumer;
//
//import lombok.extern.slf4j.Slf4j;
//import org.apache.http.HttpHost;
//import org.opensearch.action.index.IndexRequest;
//import org.opensearch.client.*;
//import org.opensearch.common.xcontent.XContentType;
//
//import java.io.IOException;
//
//@Slf4j
//public class MyOpenSearchClient {
//    private  RestHighLevelClient client;
//
//    public MyOpenSearchClient() {
//        try {
//            RestClientBuilder builder = RestClient.builder(new HttpHost("13.209.51.15", 9200, "http"));
//            this.client = new RestHighLevelClient(builder);
//        }catch (Exception e){
//            log.error(e.getMessage());
//        }
//    }
//
//    public void indexData(Object jsonData) {
//
//        try {
//            IndexRequest request = new IndexRequest("maru3");
//            //jsonData는 인덱싱할 실제 데이터, XContentType.JSON은 데이터의 콘텐츠 타입 지정
//            //request.source(jsonData, XContentType.JSON);
//            request.source(jsonData);
//        //    request.source(XContentType.JSON);
//
//        }catch (Exception e){
//            log.error(e.getMessage());
//        }
//
////        client.index(request, RequestOptions.DEFAULT);
//
////        Request request = new Request("POST", "telegraf-metrics");
////        request.setJsonEntity(jsonData);
////
////        try {
////            client.getLowLevelClient().performRequest(request);
////        } catch (IOException e) {
////            e.printStackTrace();
////        }
//    }
//
//    public void close() {
//        try {
//            client.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//}
//
