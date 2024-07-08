package org.mysamples.server;

import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.telemetry.ClientTelemetry;
import org.apache.kafka.server.telemetry.ClientTelemetryPayload;
import org.apache.kafka.server.telemetry.ClientTelemetryReceiver;

import java.util.List;
import java.util.Map;

/*
add to kafka server.properties
metric.reporters=org.mysamples.server.POCClientTelemetryReporter
 */
public class POCClientTelemetryReporter implements ClientTelemetry, MetricsReporter {

    @Override
    public void init(List<KafkaMetric> metrics) {
        System.out.println("POCClientTelemetryReporter init");
    }

    @Override
    public void metricChange(KafkaMetric metric) {

    }

    @Override
    public void metricRemoval(KafkaMetric metric) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        System.out.println("POCClientTelemetryReporter init");
    }

    private final ClientTelemetryReceiver clientReceiver = new ClientTelemetryReceiver() {
        @Override
        public void exportMetrics(AuthorizableRequestContext context, ClientTelemetryPayload payload) {
            System.out.println("**** POCClientTelemetryReporter.exportMetrics " + context + " " + payload);
        }
    };

    @Override
    public ClientTelemetryReceiver clientReceiver() {
        return clientReceiver;
    }
}
