package org.mysamples.producers;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.FenceProducersOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Level;
import org.mysamples.common.Common;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class FenceProducerTest {

    public static void main(String[] args) throws Exception {
        Common.log4jClientConfig(Level.INFO);

        Map<String, Object> adminProps = new HashMap<>();
        adminProps.put("bootstrap.servers", "localhost:9092");

        try (AdminClient admin = AdminClient.create(adminProps)) {

            NewTopic newTopic = new NewTopic("topic", 1, (short) 3);
            try {
                admin.createTopics(Collections.singleton(newTopic)).all().get();
                System.out.println("topic created");
            } catch (ExecutionException ee) {
                if (!(ee.getCause() instanceof TopicExistsException)) {
                    throw (Exception)ee.getCause();
                }
                System.out.println("topic exists");
            }

            Map<String, Object> producerProps = new HashMap<>();
            producerProps.putAll(adminProps);
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            String txId = "FenceProducerTest";
            producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txId);
//            producerProps.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 5000);

            KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

            int count = 0;
            try {
                admin.fenceProducers(Collections.singleton(txId)).all().get();

                producer.initTransactions();
                producer.beginTransaction();
                ProducerRecord<String, String> record = new ProducerRecord<>("topic", null, "" + (count++) + " " + System.currentTimeMillis());
                System.out.println(producer.send(record).get());
                producer.commitTransaction();

                FenceProducersOptions fpo = new FenceProducersOptions();
//                fpo.timeoutMs(20000);
                admin.fenceProducers(Collections.singleton(txId), fpo).all().get();

                try {
                    producer.beginTransaction();
                    System.out.println(producer.send(record).get());
                    System.out.println(producer.send(record).get());
                } catch (ExecutionException e) {
                    System.out.println("OK 1 " + e.getCause().getMessage());;
                } catch (ProducerFencedException e) {
                    System.out.println("OK 2 " + e.getMessage());;
                }

                try {
                    producer.commitTransaction();
                } catch (ProducerFencedException e) {
                    System.out.println("OK 3 " + e.getMessage());;
                } catch (InvalidProducerEpochException e) {
                    System.out.println("OK 4 " + e.getMessage());;
                }

            } catch (Exception exc) {
                exc.printStackTrace();
                throw exc;
            } finally {
                producer.close();
            }
        }
    }
}

