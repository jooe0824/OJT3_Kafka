package com.skt.test.maru3.Config;


import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class OpensearchConfig {


    final String OpensearchAddress = "13.209.51.15";
    final int OpensearchPort = 9200;

    /**
     *
     * Opensearch Address and IP Setting
     */
    @Bean
    public RestHighLevelClient client() {
        return new RestHighLevelClient(
                RestClient.builder(new HttpHost(OpensearchAddress, OpensearchPort, "http")));
    }

}
