package org.mysamples.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.UpdateFeaturesOptions;
import org.apache.kafka.clients.admin.UpdateFeaturesResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.log4j.Level;
import org.mysamples.common.Common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class MyAdminAPISnippets {

    public MyAdminAPISnippets() {
    }

    public static void main(String[] args) throws Exception {
        Common.log4jClientConfig(Level.INFO);

       Map clientProps = new HashMap<>();
       clientProps.put("bootstrap.servers", "localhost:9092");

        try (AdminClient adminClient = AdminClient.create(clientProps)) {
            // LIST TOPIC
            Set<String> topicNames = adminClient.listTopics().names().get();
            System.out.println(">>>listTopics: count=" + topicNames.size() + "\n" + topicNames);
            System.out.println();

            Set<String> internalTopicNames = topicNames.stream().filter(t -> t.startsWith("_")).collect(Collectors.toSet());
            System.out.println(">>>internal topics: count=" + internalTopicNames.size() + "\n" + internalTopicNames);
            System.out.println();

            Set<String> userTopicNames = new HashSet<>(topicNames);
            userTopicNames.removeAll(internalTopicNames);
            System.out.println(">>>user: count=" + userTopicNames.size() + "\n" + userTopicNames);
            System.out.println();

            // CREATE TOPIC
            if (true) {
                Collection<NewTopic> newTopics = new ArrayList<>();
                NewTopic nt = new NewTopic("edo-test-topic-1", Optional.of(1), Optional.of((short) 3));
                Map<String,String> configs = new HashMap<>();
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

            //DELETE TOPIC
            if (false) {
                System.out.println(">>>deletingTopics");
                adminClient.deleteTopics(topicNames).all().get();
                System.out.println(">>>deletedTopics");
            }

            // CONSUMER GROUPS
            if (false) {
                Collection<ConsumerGroupListing> cgs = adminClient.listConsumerGroups().all().get();
                System.out.println(">>>listConsumerGroups: count=" + cgs.size() + "\n" + cgs);
                System.out.println();

                var grpids = cgs.stream().map(ConsumerGroupListing::groupId).collect(Collectors.toSet());

                DescribeConsumerGroupsResult dcgr = adminClient.describeConsumerGroups(grpids);
                System.out.println(">>>describeConsumerGroups:");
                dcgr.all().get().values().forEach(cg -> System.out.println(cg));
                System.out.println();

                System.out.println(">>>listConsumerGroupOffsets:");
                grpids.forEach(gid -> {
                    ListConsumerGroupOffsetsResult lcgor = adminClient.listConsumerGroupOffsets(gid);
                    try {
                        Map<TopicPartition, OffsetAndMetadata> maptpom = lcgor.partitionsToOffsetAndMetadata().get();
                        System.out.println(gid + " -> " + maptpom);
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                });
                System.out.println();
            }


            // DESCRIBE TOPIC
            if (false) {
                Map<String, TopicDescription> topics = adminClient.describeTopics(topicNames).allTopicNames().get();
                Map<TopicPartition, OffsetSpec> earliestOffsets = new HashMap<>();
                Map<TopicPartition, OffsetSpec> latestOffsets = new HashMap<>();

                for (String t : topics.keySet()) {
                    int partitions = topics.get(t).partitions().size();
                    for (int p = 0; p < partitions; p++) {
                        TopicPartition tp = new TopicPartition(t, p);
                        earliestOffsets.put(tp, OffsetSpec.earliest());
                        latestOffsets.put(tp, OffsetSpec.latest());
                    }
                }
                System.out.println(">>>earliest:\n" + adminClient.listOffsets(earliestOffsets).all().get());
                System.out.println();
                System.out.println(">>>latest:\n" + adminClient.listOffsets(latestOffsets).all().get());
                System.out.println();
            }

            // DESCRIBE / ALTER BROKER_LOGGER
            if (false) {
                DescribeConfigsOptions options = new DescribeConfigsOptions()
                        .includeDocumentation(true);
                Collection<ConfigResource> resources = List.of(
                        new ConfigResource(ConfigResource.Type.BROKER_LOGGER, "0")
                );
                DescribeConfigsResult describeConfigs = adminClient.describeConfigs(resources, options);
                Map<ConfigResource, Config> map = describeConfigs.all().get();
                for (Map.Entry<ConfigResource, Config> entry : map.entrySet()) {
                    for (ConfigEntry configEntry : entry.getValue().entries()) {
                        System.err.println(configEntry.name() + " : " + configEntry.value());
                    }
                }
                // incremental alter BROKER_LOGGER resource
                Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
                ConfigEntry entry = new ConfigEntry("kafka.server.KafkaApis", "DEBUG");
                configs.put(new ConfigResource(ConfigResource.Type.BROKER_LOGGER, "0"),
                        Collections.singletonList(new AlterConfigOp(entry, AlterConfigOp.OpType.SET)));
                AlterConfigsResult acr = adminClient.incrementalAlterConfigs(configs);
                acr.all().get();
            }

            if (false) {
                DescribeClusterResult describeCluster = adminClient.describeCluster();
                Collection<Node> nodes = describeCluster.nodes().get();
                System.out.println(">>>describeCluster.nodes:\n" + nodes);
                System.out.println();

                List<ConfigResource> brokerResources = new ArrayList<>();
                for (Node node : nodes) {
                    brokerResources.add(new ConfigResource(ConfigResource.Type.BROKER, node.idString()));
                }
                Map<ConfigResource, Config> dcrBrokers = adminClient.describeConfigs(brokerResources).all().get();
                System.out.println(">>>describeConfigs(brokerResources):\n" + dcrBrokers);
                System.out.println();

                Map<String, TopicDescription> desc = adminClient.describeTopics(topicNames).allTopicNames().get();
                System.out.println(">>>describeTopics:\n" + desc);
                System.out.println();

                List<ConfigResource> topicResources = new ArrayList<>();
                for (String topic : topicNames) {
                    topicResources.add(new ConfigResource(ConfigResource.Type.TOPIC, topic));
                }
                Map<ConfigResource, Config> dcrTopics = adminClient.describeConfigs(topicResources).all().get();
                System.out.println(">>>describeConfigs(topicResources):\n" + dcrTopics);
                System.out.println();

                List<Integer> brokers = new ArrayList<>();
                for (Node node : nodes) {
                    brokers.add(node.id());
                }
                DescribeLogDirsResult describeLogDirs = adminClient.describeLogDirs(brokers);
                Map<Integer, Map<String, LogDirDescription>> describeLogDirsGet = describeLogDirs.allDescriptions().get();
                System.out.println(">>>describeLogDirs:\n" + describeLogDirsGet);
                System.out.println();

                Collection<TopicPartitionReplica> tprs = new ArrayList<>();
                for (String topic : topicNames) {
                    for (Node node : nodes) {
                        tprs.add(new TopicPartitionReplica(topic, 0, node.id()));
                    }
                }
                Map<TopicPartitionReplica, ReplicaLogDirInfo> drld = adminClient.describeReplicaLogDirs(tprs).all().get();
                System.out.println(">>>describeReplicaLogDirs:\n" + drld);
                System.out.println();
                //            AccessControlEntry entry = new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.WRITE, AclPermissionType.ALLOW);
                //            AclBinding aclBinding = new AclBinding(Resource.CLUSTER, entry);
                //            Collection<AclBinding> acls = Collections.singletonList(aclBinding);
                //            CreateAclsResult aclsResult = adminClient.createAcls(acls);
                //            for(Map.Entry<AclBinding, KafkaFuture<Void>> mapentry : aclsResult.values().entrySet()) {
                //                System.out.println(mapentry.getKey());
                //                mapentry.getValue().get();
                //            }
            }


            if (false) {
                Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
                ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, "edo-test");
                ConfigEntry entry = new ConfigEntry("cleanup.policy", "compact");
                AlterConfigOp acop = new AlterConfigOp(entry, AlterConfigOp.OpType.SET);
                configs.put(configResource, Collections.singleton(acop));
                AlterConfigsResult alterConfigsResult = adminClient.incrementalAlterConfigs(configs);
                Map<ConfigResource, KafkaFuture<Void>> alterConfigFutures = alterConfigsResult.values();
                for (ConfigResource cr : alterConfigFutures.keySet()) {
                    try {
                        alterConfigFutures.get(cr).get(10, TimeUnit.SECONDS);
                        System.out.println("Config " + cr + " altered");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            if (false) {
                Map<String, NewPartitions> newPartitionsMap = new HashMap<>();
                NewPartitions newPartitions = NewPartitions.increaseTo(60);
                newPartitionsMap.put("edo-test-topic3-3parts", newPartitions);
                CreatePartitionsResult createPartitionsResult = adminClient.createPartitions(newPartitionsMap);
                Map<String, KafkaFuture<Void>> createPartitionsFutures = createPartitionsResult.values();
                for (String topic : createPartitionsFutures.keySet()) {
                    try {
                        createPartitionsFutures.get(topic).get(10, TimeUnit.SECONDS);
                        System.out.println("Partitions for " + topic + " increased");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            if (false) {
                Map<ConfigResource, Config> configs = new HashMap<>();
                ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, "streams-wordcount-Counts-changelog");
                ConfigEntry entry = new ConfigEntry("segment.bytes", "86400000");
                Config config = new Config(Collections.singletonList(entry));
                configs.put(configResource, config);
                System.out.println("Altering `full Config " + configs);
                AlterConfigsResult alterConfigsResult = adminClient.alterConfigs(configs);
                Map<ConfigResource, KafkaFuture<Void>> acf = alterConfigsResult.values();
                for (ConfigResource cr : acf.keySet()) {
                    acf.get(cr).get(10, TimeUnit.SECONDS);
                    System.out.println("Config " + cr + " altered");
                }
            }

            if (false) {
                Map<TopicPartitionReplica, String> replicaAssignment = new HashMap<>();
                TopicPartitionReplica tpr = new TopicPartitionReplica("9e9e12c0streams-wordcount-Counts-changelog", 0, 0);
                replicaAssignment.put(tpr, "/var/log");

                try {
                    System.out.println(">>>adminClient.alterReplicaLogDirs");
                    adminClient.alterReplicaLogDirs(replicaAssignment).all().get();
                    System.out.println("<<<adminClient.alterReplicaLogDirs");
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }

            if (false) {
                Map<String, FeatureUpdate> featureUpdates = new HashMap<>();
                featureUpdates.put("feature1", new FeatureUpdate((short) 5, FeatureUpdate.UpgradeType.UPGRADE));
                UpdateFeaturesResult updateFeatures = adminClient.updateFeatures(featureUpdates, new UpdateFeaturesOptions());
                updateFeatures.all().get();
            }

        } finally {
            Thread.sleep(2000L);
        }
    }

}

