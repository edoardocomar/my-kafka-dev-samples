package org.mysamples.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.mysamples.common.Common;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;


public class MyConsumer {
    static Logger logger = LoggerFactory.getLogger(MyConsumer.class);

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Common.log4jClientConfig(Level.INFO);

        Map clientProps = new HashMap<>();
        clientProps.put("bootstrap.servers", "localhost:9092");

        clientProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        clientProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        clientProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        clientProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        clientProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        clientProps.put(ConsumerConfig.GROUP_ID_CONFIG, "mygrp6");
//        clientProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
//        clientProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed");
//        clientProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,"1");
//        clientProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "10");
//        clientProps.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,"1024");
//        clientProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");

        final String[] TOPICs = new String[]{"topic100"};
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(clientProps);

        try {
            consumer.subscribe(Arrays.asList(TOPICs)
                    , new ConsumerRebalanceListener() {

                        @Override
                        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                            System.out.println("onPartitionsRevoked1:" + partitions);
                        }

                        @Override
                        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                            System.out.println("onPartitionsAssigned1:" + partitions);
                        }
                    });

            long consumed = 0;
            boolean terminate = false;
            int pollCount = 0;

            while (true) { // !consumedAll(consumer)) {
                ConsumerRecords<String, String> crs1 = consumer.poll(Duration.ofMillis(1000L));
                pollCount++;
                try {
                    print(crs1, consumer);
                    if(!crs1.isEmpty()) {
//                        consumer.commitSync(Duration.ofSeconds(10));
//                        System.out.println("committed " + consumer.committed(consumer.assignment()));
                        consumer.commitAsync(new OffsetCommitCallback() {
                            @Override
                            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                                System.out.println("onComplete " + offsets + " " + exception);
                            }
                        });
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
                Thread.sleep(500L);
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        } finally {
            System.out.println("consumer closing " + new Date());
            consumer.close();
            System.out.println("consumer closed " + new Date());
        }
    }

    private static boolean consumedAll(KafkaConsumer<String, String> consumer) {
        Set<TopicPartition> tps = consumer.assignment();
        if(tps.isEmpty()) {
            return false;
        }

        boolean result = true;
        for(TopicPartition tp : tps) {
            OptionalLong oLag = consumer.currentLag(tp);
            if (oLag.isEmpty() || oLag.getAsLong()!=0) {
                result = false;
            }
        }
        return result;
    }

    private static void print(ConsumerRecords<String, String> crs, KafkaConsumer<String, String> consumer) {
        System.out.println(new Date() + " poll count=" + crs.count());
        Iterator<ConsumerRecord<String, String>> iterator = crs.iterator();
//        if(iterator.hasNext()) { //print first
//            ConsumerRecord<String, String> cr = iterator.next();
//            System.out.println(cr.topic() + " key=" + new String(cr.key()) + ", " + cr.partition() + ", offset=" + cr.offset() + " value=" + new String(cr.value()));
//        }

        int num = 0;
        for (ConsumerRecord<String, String> cr : crs) {
            num++;
            if (num == crs.count()) { ////print last polled
                System.out.println(cr.topic() + " key=" + cr.key() + ", " + cr.partition() + ", offset=" + cr.offset() + " value=" + cr.value());
            }
        }
    }
}


