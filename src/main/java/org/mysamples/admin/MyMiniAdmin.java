package org.mysamples.admin;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.log4j.Level;
import org.mysamples.common.Common;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MyMiniAdmin {

    public MyMiniAdmin() {
    }

    public static void main(String[] args) throws Exception {
        Common.log4jClientConfig(Level.WARN);

        Map clientProps = new HashMap<>();
        clientProps.put("bootstrap.servers", "localhost:9092");

        clientProps.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        clientProps.put(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000");

        long start = -1;
        try (AdminClient adminClient = AdminClient.create(clientProps)) {

            // LIST TOPIC
            Set<String> topicNames = adminClient.listTopics().names().get();
            System.out.println(">>>listTopics: count=" + topicNames.size() + "\n" + topicNames);
            System.out.println();

//            Collection<NewTopic> newTopics = new ArrayList<>();
//            NewTopic nt = new NewTopic("Ztopic1", Optional.of(1), Optional.of((short) 3));
//            Map<String,String> configs = new HashMap<>();
//            nt.configs(configs);
//            newTopics.add(nt);
//            Map<String, KafkaFuture<Void>> result = adminClient.createTopics(newTopics).values();
//            for (String t : result.keySet()) {
//                try {
//                    System.out.println(t + " " + result.get(t).get());
//                } catch (Exception ex) {
//                    ex.printStackTrace();
//                }
//            }

        } finally {
            long end = System.currentTimeMillis();
            System.out.println("FENCE TIMEOUT AFTER = " + ((end-start)/1000));
            Thread.sleep(2000L);
        }
    }

}

