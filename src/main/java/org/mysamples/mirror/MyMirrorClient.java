package org.mysamples.mirror;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.mirror.MirrorClient;
import org.apache.kafka.connect.mirror.MirrorClientConfig;
import org.apache.kafka.connect.mirror.MirrorMakerConfig;
import org.apache.kafka.connect.mirror.RemoteClusterUtils;
import org.apache.log4j.Level;
import org.mysamples.common.Common;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MyMirrorClient {

    public static void main(String[] args) throws Exception {
        Common.log4jClientConfig(Level.WARN);

        Map<String,String> m2propsMap = new HashMap<>();
        m2propsMap.put("clusters","source, target");
        m2propsMap.put("source.bootstrap.servers","localhost:9092");
        m2propsMap.put("target.bootstrap.servers","localhost:9192");

        MirrorMakerConfig mmConfig = new MirrorMakerConfig(m2propsMap);
        MirrorClientConfig mmClientConfig = mmConfig.clientConfig("target");

        // these works only if heartbeats are enabled
        Set<String> upstreamClusters = RemoteClusterUtils.upstreamClusters(mmClientConfig.originals());
        System.out.println("RemoteClusterUtils.upstreamClusters = " + upstreamClusters);
        int replicationHops = RemoteClusterUtils.replicationHops(mmClientConfig.originals(), "source");
        System.out.println("RemoteClusterUtils.replicationHops = " + replicationHops);

        Map<TopicPartition, OffsetAndMetadata> translateOffsets =
                RemoteClusterUtils.translateOffsets(mmClientConfig.originals(), "source", "mygroup1", Duration.ofMillis(3000));
        System.out.println("RemoteClusterUtils.translateOffsets = " + translateOffsets);

        MirrorClient mmClient = new MirrorClient(mmClientConfig);
        System.out.println("mmClient.remoteTopics()=" + mmClient.remoteTopics());
        Map<TopicPartition, OffsetAndMetadata> remoteConsumerOffset2 = mmClient.remoteConsumerOffsets("mygroup1", "source", Duration.ofMillis(3000));
        System.out.println("mmClient.remoteConsumerOffsets=" + remoteConsumerOffset2);
    }

}
