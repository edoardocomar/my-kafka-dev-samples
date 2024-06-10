package org.mysamples.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * consume some records then pause, so the group stays active
 */
public class MySlowConsumer {

    public static void main(String[] args) throws InterruptedException {
        Map clientProps = new HashMap<>();
        clientProps.put("bootstrap.servers", "localhost:9092");

        clientProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        clientProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        clientProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        clientProps.put(ConsumerConfig.GROUP_ID_CONFIG, "mygrp1");

        clientProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");

        final String[] TOPICs = new String[]{"mytopic"};
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(clientProps);
        try {
            consumer.subscribe(Arrays.asList(TOPICs), new ConsumerRebalanceListener() {
                        @Override
                        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                            System.out.println("onPartitionsRevoked1:" + partitions);
                        }
                        @Override
                        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                            System.out.println("onPartitionsAssigned1:" + partitions);
                            if (!partitions.isEmpty()) {
                                consumer.seek(partitions.iterator().next(), 10);
                                consumer.commitSync();
                                consumer.pause(partitions);
                            }
                        }
                    });

            boolean paused = false;
            int pollCount = 0;
            int polledCount = 0;
            long now = System.currentTimeMillis();
            while (System.currentTimeMillis() - now < 10*1000L) {
                ConsumerRecords<String, String> crs1 = consumer.poll(Duration.ofMillis(1000L));
                pollCount++;
                try {
                    polledCount = polledCount + print(crs1, consumer);
                    consumer.commitSync(Duration.ofSeconds(10));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                Thread.sleep(1000L);

//                if (polledCount >=500 && !paused) {
//                    consumer.pause(consumer.assignment());
//                    paused = true;
//                    break;
//                }
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

    private static int print(ConsumerRecords<String, String> crs, KafkaConsumer<String, String> consumer) {
        System.out.println(new Date() + " poll count=" + crs.count());
        Iterator<ConsumerRecord<String, String>> iterator = crs.iterator();
        int num = 0;
        for (ConsumerRecord<String, String> cr : crs) {
            num++;
            if (num == crs.count()) { ////print last polled
                System.out.println(cr.topic() + " key=" + cr.key() + ", " + cr.partition() + ", offset=" + cr.offset() + " value=" + cr.value());
            }
        }
        return crs.count();
    }

}
