package org.mysamples.clitools;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConfigEntry.ConfigSource;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.log4j.Level;
import org.mysamples.common.Common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Sample DescribeTopics CLI app using Kafka 1.1 Admin API
 * <p>
 * Mimics the kafka-topics.sh --describe script but without requiring zookeeper access
 */
public class DescribeTopics {
    public static void main(String[] args) throws Exception {

        Common.log4jClientConfig(Level.INFO);

        Map clientProps = new HashMap<>();
        clientProps.put("bootstrap.servers", "localhost:9092");

        AdminClient adminClient = AdminClient.create(clientProps);
        try {
            // get all topic names
            ListTopicsOptions option = new ListTopicsOptions().listInternal(true);
            ListTopicsResult listTopics = adminClient.listTopics(option);
            KafkaFuture<Set<String>> listTopicsFuture = listTopics.names();
            Set<String> allTopicNames = listTopicsFuture.get();

            // get descriptions (partitions, replica, isr) for all topics 
            DescribeTopicsResult describeTopics = adminClient.describeTopics(allTopicNames);
            KafkaFuture<Map<String, TopicDescription>> describeTopicsFuture = describeTopics.all();
            Map<String, TopicDescription> allTopicDescriptions = describeTopicsFuture.get();

            // get configuration for all topics 
            Collection<ConfigResource> resources = new ArrayList<ConfigResource>();
            for (String topic : allTopicNames) {
                resources.add(new ConfigResource(Type.TOPIC, topic));
            }
            DescribeConfigsResult describeConfigs = adminClient.describeConfigs(resources);
            KafkaFuture<Map<ConfigResource, Config>> describeConfigsFuture = describeConfigs.all();
            Map<ConfigResource, Config> allTopicConfigs = describeConfigsFuture.get();

            for (ConfigResource crt : allTopicConfigs.keySet()) {
                StringBuilder header = new StringBuilder();
                String topic = crt.name();
                header.append("Topic: ").append(topic);
                Config config = allTopicConfigs.get(crt);
                TopicDescription desc = allTopicDescriptions.get(topic);
                if(desc !=null) {
                    header.append("  PartitionCount:").append(desc.partitions().size());
                    if (desc.partitions().size() > 0)
                        header.append("  ReplicationFactor:").append(desc.partitions().get(0).replicas().size());
                }

                if (config!=null) {
                    header.append("  Configs:");
                    for (ConfigEntry configEntry : config.entries()) {
                        if(ConfigSource.DYNAMIC_TOPIC_CONFIG == configEntry.source()) {
                            header.append(configEntry.name()).append("=").append(configEntry.value()).append(", ");
                        }
                    }
                }
                truncateEnd(header, ", ");
                addPartitionSize(config, header);
                System.out.println(header);

                if (desc != null) {
                    for (TopicPartitionInfo tpInfo : desc.partitions()) {
                        StringBuilder line = new StringBuilder();
                        line.append("\tTopic: ").append(topic).append("  Partition: ").append(tpInfo.partition());
                        
                        line.append("  Leader: ").append(tpInfo.leader().idString());

                        List<Node> replicas = tpInfo.replicas();
                        line.append("  Replicas: ");
                        for (Node node : replicas) {
                            line.append(node.idString()).append("(").append(node.rack()).append("),");
                        }
                        truncateEnd(line, ",");

                        List<Node> isr = tpInfo.isr();
                        line.append("  ISR: ");
                        for (Node node : isr) {
                            line.append(node.idString()).append(",");
                        }
                        truncateEnd(line, ",");
                        System.out.println(line);
                    }
                }
                System.out.println();
            }
        } finally {
            Thread.sleep(2000L);
            adminClient.close();
        }
    }

    private static void addPartitionSize(Config config, StringBuilder header) {
        try {
            long retentionSize = Long.parseLong(config.get(TopicConfig.RETENTION_BYTES_CONFIG).value());
            int segmentSize = Integer.parseInt(config.get(TopicConfig.SEGMENT_BYTES_CONFIG).value());
            int maxIndexSize = Integer.parseInt(config.get(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG).value());

            long partitionSize = maxBytesOnDisk(retentionSize, segmentSize, maxIndexSize);
            header.append("  MaxPartitionSize:" + formatSize(partitionSize));
        } catch (Exception exc) {
            header.append("  MaxPartitionSize:N/A");
        }
    }

//    static void log4jClientConfig(Level level) {
//        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
//        Configuration config = ctx.getConfiguration();
//        LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
//        loggerConfig.setLevel(level);
//        ctx.updateLoggers();
//    }

    static void truncateEnd(StringBuilder sb, String suffix) {
        if(suffix.equals(sb.substring(sb.length()-suffix.length(), sb.length()))) {
            sb.replace(sb.length()-suffix.length(),sb.length(), "");
        }
    }

    static String formatSize(long v) {
        if (v < 1024) return v + " B";
        int z = (63 - Long.numberOfLeadingZeros(v)) / 10;
        return String.format("%.1f%sB", (double)v / (1L << (z*10)), " KMGTPE".charAt(z));
    }

    static long maxBytesOnDisk(long retentionSize, int segmentSize, int maxIndexSize) {

        long numSegments = retentionSize / segmentSize + 1L;

        // if retention is not an exact multiple of segment, we can get an extra segment
        if (retentionSize % segmentSize != 0) {
            numSegments++;
        }

        // the two indexes for each segment have roughly the same size,
        // bounded by the same maxIndexSize
        long maxIndexesSize = numSegments * 2 * maxIndexSize;

        long result = retentionSize + segmentSize + maxIndexesSize;

        return result;
    }

}
