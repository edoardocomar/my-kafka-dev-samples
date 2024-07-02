package org.mysamples.common;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.consumer.internals.RequestFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ListGroupsRequest;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


public class MyDirectRequest {
    private static final Logger LOG = LoggerFactory.getLogger(MyDirectRequest.class);

    public static void main(String[] args) throws Exception {
        Common.log4jClientConfig(Level.INFO);

        Map clientProps = new HashMap<>();
        clientProps.put("bootstrap.servers", "localhost:9092");

        clientProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        clientProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try {
            MetadataRequestData data = new MetadataRequestData();
            data.setTopics(null);
            data.setIncludeTopicAuthorizedOperations(true);
            AbstractRequest.Builder<?> requestBuilder = new MetadataRequest.Builder(data);

            ClientResponse cr = sendAndPoll(clientProps, requestBuilder, Optional.ofNullable(null));
            MetadataResponse mr = (MetadataResponse) cr.responseBody();
            MetadataResponseData mdrd = mr.data();

            System.out.println(" ");
            System.out.println(mdrd);
        } finally {
            Thread.sleep(2000L);
        }

    }

    public static ClientResponse sendAndPoll(Map clientProps, AbstractRequest.Builder<?> requestBuilder, Optional<Node> oNode)
            throws Exception {

        try(KafkaConsumer consumer = new KafkaConsumer<>(clientProps)) {
            ConsumerNetworkClient cnc = getConsumerNetworkClient(consumer);
            Node node = oNode.orElse(cnc.leastLoadedNode());
            LOG.info("Sending arbitrary request to node {}",node);

            // the network client checks that the request to be sent fit in the set that the broker supports
            // to circumvent the check, we first send a trivial request (e.g. ListGroups) that causes the client to
            // fill an "ApiVersions" map. We then remove the node id from that map, so that the check will be skipped
            RequestFuture<ClientResponse> f0 = cnc.send(node, new ListGroupsRequest.Builder(new ListGroupsRequestData()));
            cnc.poll(f0, Time.SYSTEM.timer(Duration.ofSeconds(30)));
            assertTrue(f0.isDone());

            Field networkClientField = cnc.getClass().getDeclaredField("client");
            networkClientField.setAccessible(true);
            NetworkClient nc = (NetworkClient) networkClientField.get(cnc);
            Field apiVersionField = nc.getClass().getDeclaredField("apiVersions");
            apiVersionField.setAccessible(true);
            ApiVersions apiVersions = (ApiVersions) apiVersionField.get(nc);
            apiVersions.remove(node.idString());

            RequestFuture<ClientResponse> future = cnc.send(node, requestBuilder);
            cnc.poll(future, Time.SYSTEM.timer(Duration.ofSeconds(30)));
            assertTrue(future.isDone());

            if (!future.succeeded()) {
                throw future.exception();
            }

            ClientResponse response = future.value();
            return response;
        }
    }

    private static ConsumerNetworkClient getConsumerNetworkClient(KafkaConsumer consumer) throws NoSuchFieldException, IllegalAccessException {

        Field[] fields = consumer.getClass().getDeclaredFields();
        for (Field field : fields) {
            if (field.getName().equals("client") && field.getType().equals(ConsumerNetworkClient.class)) {
                field.setAccessible(true);
                return (ConsumerNetworkClient) field.get(consumer);
            }

            // 3.7 and later
            if (field.getName().equals("delegate") && field.getType().getSimpleName().equals("ConsumerDelegate")) {
                field.setAccessible(true);
                Object consumerDelegate = field.get(consumer);
                Field cdfield = consumerDelegate.getClass().getDeclaredField("client");
                cdfield.setAccessible(true);
                return (ConsumerNetworkClient) cdfield.get(consumerDelegate);
            }
        }
        throw new NoSuchFieldException("Can't find ConsumerNetworkClient by introspecting ");
    }

    private static void assertTrue(boolean done) {
        if (!done) {
            throw new RuntimeException("Assertion failed");
        }
    }

}
