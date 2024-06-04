package com.skt.test.maru3.data;
import lombok.Data;
import com.google.gson.Gson;

import java.util.Map;

/**
 * Class for 'JSon to Object'
 * fields의 key 값이 매번 상이하기 때문에 Map 으로 설정
 */

@Data
public class KafkaData {
    private Map<String, String> fields;
    private String name;
    private Tags tags;
    private Long timestamp;


    @Data
    static public class Tags{
        private String device;
        private String host;
        private String irq;
        private String type;
        private String tags;
    }

}
