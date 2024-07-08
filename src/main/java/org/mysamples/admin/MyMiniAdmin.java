package org.mysamples.admin;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.log4j.Level;
import org.mysamples.common.Common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class MyMiniAdmin {

    public MyMiniAdmin() {
    }

    public static void main(String[] args) throws Exception {
        Common.log4jClientConfig(Level.WARN);

        Map clientProps = new HashMap<>();
        clientProps.put("bootstrap.servers", "localhost:9092");

        try (AdminClient adminClient = AdminClient.create(clientProps)) {

            // LIST TOPIC
            Set<String> topicNames = adminClient.listTopics().names().get();
            System.out.println(">>>listTopics: count=" + topicNames.size() + "\n" + topicNames);

            if (!topicNames.contains("mytopic1")) {
                Collection<NewTopic> newTopics = new ArrayList<>();
                NewTopic nt = new NewTopic("mytopic1", Optional.of(1), Optional.of((short) 1));
                Map<String, String> configs = new HashMap<>();
                nt.configs(configs);
                newTopics.add(nt);
                Map<String, KafkaFuture<Void>> result = adminClient.createTopics(newTopics).values();
                for (String t : result.keySet()) {
                    try {
                        System.out.println(t + " " + result.get(t).get());
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            }
            
        } finally {
            Thread.sleep(60000L);
        }
    }

}

