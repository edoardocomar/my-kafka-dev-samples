package org.mysamples.clitools;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.requests.DescribeLogDirsResponse.LogDirInfo;
import org.apache.kafka.common.requests.DescribeLogDirsResponse.ReplicaInfo;
import org.apache.log4j.Level;
import org.mysamples.common.Common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class TopicOffsetsAndSizes {
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Common.log4jClientConfig(Level.WARN);

        Map clientProps = new HashMap<>();
        clientProps.put("bootstrap.servers", "localhost:9092");
 
        AdminClient adminClient = AdminClient.create(clientProps);
        clientProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        clientProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(clientProps);;
        try {
            Set<String> topicNames = adminClient.listTopics().names().get();
            System.out.println(">>>listTopics:\n" + topicNames);
            System.out.println();

            DescribeClusterResult describeCluster = adminClient.describeCluster();
            Collection<Node> nodes = describeCluster.nodes().get();

            List<ConfigResource> brokerResources = new ArrayList<>();
            for (Node node : nodes) {
                brokerResources.add(new ConfigResource(ConfigResource.Type.BROKER, node.idString()));
            }

//            topicNames = Collections.singleton("primary.edo-test-topic-3parts");
            Map<String, TopicDescription> descTopics = adminClient.describeTopics(topicNames).all().get();

            Collection<TopicPartition> tps = new ArrayList<>();
            for (Map.Entry<String, TopicDescription> entry : descTopics.entrySet()) {
                int partitionCount = entry.getValue().partitions().size();
                for (int i=0; i<partitionCount; i++) {
                    TopicPartition tp = new TopicPartition(entry.getKey(), i);
                    tps.add(tp);
                }
            }
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(tps);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(tps);

            List<Integer> brokers = new ArrayList<>();
            for (Node node : nodes) {
                brokers.add(node.id());
            }
            DescribeLogDirsResult describeLogDirs = adminClient.describeLogDirs(brokers);
            Map<Integer, Map<String, LogDirInfo>> describeLogDirsGet = describeLogDirs.all().get();

            System.out.println(">>>topicPartition messages sizes-per-broker:");
            for (TopicPartition tp : tps) {
                StringBuilder sb = new StringBuilder();
                long endOffset = endOffsets.get(tp);
                long beginningOffset = beginningOffsets.get(tp); 
                long messages = endOffset - beginningOffset;
                sb.append(tp).append(" num messages=end-begin : ").append(messages).append(" = ").append(endOffset).append("-").append(beginningOffset);
                sb.append("   broker/size=");
                for(int broker : describeLogDirsGet.keySet()) {
                    Map<TopicPartition, ReplicaInfo> replicaInfos = describeLogDirsGet.get(broker).values().iterator().next().replicaInfos;
                    ReplicaInfo replicaInfo = replicaInfos.get(tp);
                    if(replicaInfo!=null) {
                        sb.append(broker).append("/").append(replicaInfo.size).append(",");
                    }
                }
                sb.delete(sb.length()-1, sb.length());
                System.out.println(sb);
            }
        } finally {
            Thread.sleep(2000L);
            consumer.close();
            adminClient.close();
        }
    }

}
