package org.mysamples.producers;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Level;
import org.mysamples.common.Common;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

public class MyProducer {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {

        Common.log4jClientConfig(Level.INFO);

        Map<String, Object> clientProps = new HashMap();
        clientProps.put("bootstrap.servers", "localhost:9092");

        clientProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer") ;
        clientProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        clientProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "23000");
//        clientProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
//        clientProps.put(ProducerConfig.BATCH_SIZE_CONFIG, "100000");
//        clientProps.put(ProducerConfig.LINGER_MS_CONFIG, "500");
//        clientProps.put("acks", "1");
        clientProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"mytxn");
//        clientProps.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,"38000");

        KafkaProducer<String, String> producer = new KafkaProducer<>(clientProps);
        try {
            final String[] TOPICs = new String[]{"mytopic2"};

            producer.initTransactions();
            producer.beginTransaction();
//            Random random = new Random();

            long count = 0;
            final long MAXREC = 1;
            String bodyTail = " 9 COMMITTED";//""-"+makeString(10);
            Future<RecordMetadata> future = null;
            while(count < MAXREC) {
                final String topic = TOPICs[(int) (count % TOPICs.length)];

//                int partition = (int) (count % 3);
                String value = "value-" + System.currentTimeMillis() + "-" + String.format("%06d", (Long)count) + " " + bodyTail;
                String key = null;// "key-" + String.format("%06d", (Long)count);

                ProducerRecord<String, String> recordP = new ProducerRecord(topic, key, value);

                future = producer.send(recordP, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception exc) {
                        if (exc != null) {
                            System.err.println(key + " : " + recordMetadata + "\t" + exc);
                        } else {
                            //System.err.println(recordMetadata.offset());
                        }
                    }
                });

//                if (count % MAXREC == (MAXREC-1)) {
                if (count % 1000 == 999) {
                    try {
                        RecordMetadata r = future.get();
                        System.out.println(new Date() + " count=" + count + " offset=" + r.offset() + " [" + r.topic() + "-" + r.partition() + "]");
//                        Thread.sleep(100L);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                count++;
            }
            RecordMetadata r = future.get();
            System.out.println(new Date() + " count=" + count + " offset=" + r.offset() + " [" + r.topic() + "-" + r.partition() + "]");

            if(bodyTail.endsWith("COMMITTED")) {
                producer.commitTransaction();
            } else if (bodyTail.endsWith("ABORTED")) {
                producer.abortTransaction();
            }

//            List<String> metricsOfInterest = List.of("request-latency-max","outgoing-byte-rate");
//            Map<MetricName, ? extends Metric> metrics = producer.metrics();
//            for (MetricName mn  : metrics.keySet()) {
//                if(metricsOfInterest.contains(mn.name()) && "producer-metrics".equals(mn.group())) {
//                    KafkaMetric km = (KafkaMetric)metrics.get(mn);
//                    System.out.println(mn.name() + " : " + km.metricValue());
//                }
//            }
        } finally {
//            Thread.sleep(2000L);
            producer.close();
        }
    }

    static String makeString(int size) {
        StringBuilder sb = new StringBuilder(size);
        for (int i=0; i<size; i++) {
            sb.append('q');
        }
        return sb.toString();
    }

}

