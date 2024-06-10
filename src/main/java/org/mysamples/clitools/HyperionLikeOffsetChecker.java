package org.mysamples.clitools;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

public class HyperionLikeOffsetChecker {

    static class MyRecord {
        int partition;
        String key;
        String value;
        Headers headers;
        long timestamp;
        long offset;

        public MyRecord(int partition, String key, String value, Headers headers, long timestamp, long offset) {
            super();
            this.partition = partition;
            this.key = key;
            this.value = value;
            this.headers = headers;
            this.timestamp = timestamp;
            this.offset = offset;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((key == null) ? 0 : key.hashCode());
            result = prime * result + (int) (offset ^ (offset >>> 32));
            result = prime * result + partition;
            result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
            result = prime * result + ((value == null) ? 0 : value.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            MyRecord other = (MyRecord) obj;
            if (headers == null) {
                if (other.headers != null)
                    return false;
            } else if (!compare(headers, other.headers))
                return false;
            if (key == null) {
                if (other.key != null)
                    return false;
            } else if (!key.equals(other.key))
                return false;
            if (offset != other.offset)
                return false;
            if (partition != other.partition)
                return false;
            if (timestamp != other.timestamp)
                return false;
            if (value == null) {
                if (other.value != null)
                    return false;
            } else if (!value.equals(other.value))
                return false;
            return true;
        }

        private boolean compare(Headers h1, Headers h2) {
            Map<String, String> map1 = toMap(h1);
            Map<String, String> map2 = toMap(h2);
            return map1.equals(map2);
        }

        private Map<String, String> toMap(Headers hs) {
            Map<String, String> map = new HashMap<>();
            for (Header header : hs.toArray()) {
                if (!"offset".equals(header.key())) {
                    map.put(header.key(), new String(header.value()));
                }
            }
            return map;
        }

        @Override
        public String toString() {
            return "MyRecord [partition=" + partition + ", key=" + key + ", value=" + value + ", headers="
                    + toMap(headers) + ", timestamp=" + timestamp + ", offset=" + offset + "]";
        }
    }

    static final String TOPIC = "edotesttopic";

    public static void main(String[] args) throws Exception {
        final ConcurrentHashMap<String, MyRecord> map = new ConcurrentHashMap<>();

//        Common.log4jClientConfig(Level.INFO);
        Map clientProps = new HashMap<>();
        clientProps.put("bootstrap.servers", "localhost:9092");

        clientProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        clientProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        clientProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        clientProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // producer
        clientProps.put("linger.ms", "500");
        clientProps.put("retries", "1");
        clientProps.put("compression.type", "gzip");

        // consumer
        // clientProps.put("auto.offset.reset", "earliest");
        clientProps.put("group.id", "edo-hyperion-groupid" + System.currentTimeMillis());

        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(clientProps);
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(clientProps);
        consumer.subscribe(Arrays.asList(TOPIC), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // System.out.println("onPartitionsRevoked1:"+partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // System.out.println("onPartitionsAssigned1:"+partitions);
            }
        });

        new Thread() {
            Random rnd = new Random();

            public void run() {
                long count = 0;
                try {
                    try {
                        while (count < 1000L) {
                            String key = "key-" + count + "-" + System.currentTimeMillis();
                            String value = "value-" + count + "-" + System.currentTimeMillis();
                            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, key,
                                    value);
                            record.headers().add("count", Long.toString(count).getBytes());
                            Future<RecordMetadata> future = producer.send(record, new Callback() {
                                @Override
                                public void onCompletion(RecordMetadata rm, Exception exc) {
                                    if (exc == null) {// System.err.println(key + " : " + recordMetadata + "\t" + exc);
                                        map.put(record.key(), new MyRecord(rm.partition(), record.key(), record.value(),
                                                record.headers(), rm.timestamp(), rm.offset()));
                                    } else {
                                        exc.printStackTrace();
                                    }
                                }
                            });
                            count++;
                            if (count % (1 + rnd.nextInt(29)) == 0) {
                                Thread.sleep(1000L);
                            }
                        }
                    } finally {
                        Thread.sleep(100000L);
                        producer.close();
                    }
                } catch (Exception exc) {
                    exc.printStackTrace();
                }
            }
        }.start();

        while (true) {
            try {
                ConsumerRecords<String, String> consRecs = consumer.poll(100L);
                // ConsumerRecords<String, String> consRecs =
                // consumer.poll(Duration.ofMillis(100L));
                check(consRecs, map);
            } catch (Exception ex) {
                ex.printStackTrace();
                // break;
                Thread.sleep(2000L);
            }
        }
    }

    private static void check(ConsumerRecords<String, String> consRecs, final ConcurrentHashMap<String, MyRecord> map) {
        consRecs.forEach(cr -> {
            MyRecord sent = map.remove(cr.key());
            if (sent != null) {
                MyRecord received = new MyRecord(cr.partition(), cr.key(), cr.value(), cr.headers(), cr.timestamp(),
                        cr.offset());
                if (!received.equals(sent)) {
                    System.err.println("ERROR\n" + "RCV:" + received + "\n" + "SNT:" + sent);
                }
            }
        });
        if (consRecs.count() > 0) {
            System.out.println("ok " + consRecs.count());
        }
    }

    static String makeString(int size) {
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            sb.append('c');
        }
        return sb.toString();
    }

    public static byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }
}
