package com.skt.test.maru3.data;
import lombok.Data;
import com.google.gson.Gson;

import java.util.Map;

/**
 * Class for 'JSon to Object'
 */
//@Data
//public class KafkaData {
//    private Fields fields;
//    private String name;
//    private Tags tags;
//    private Long timestamp;
//
//    @Data static public class Fields{
//        int CPU0;
//        int CPU1;
//        int CPU2;
//        int CPU3;
//        int CPU4;
//        int CPU5;
//        int CPU6;
//        int CPU7;
//        Long total;
//      //  private String message;
//    }
//
//    @Data static public class Tags{
//        private String device;
//        private String host;
//        private String irq;
//        private String type;
////        private String tags;
//    }
@Data
public class KafkaData {
    private Map<String, String> fields;
    private String name;
    private Tags tags;
    private String timestamp;


    @Data
    static public class Tags{
        private String device;
        private String host;
        private String irq;
        private String type;
        private String tags;
    }

}
