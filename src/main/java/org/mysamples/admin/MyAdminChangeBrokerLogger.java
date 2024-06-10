package org.mysamples.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.log4j.Level;
import org.mysamples.common.Common;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class MyAdminChangeBrokerLogger {
    private static final Logger LOG = LoggerFactory.getLogger(MyAdminChangeBrokerLogger.class);

    public static void main(String[] args) throws Exception {
        Common.log4jClientConfig(Level.INFO);

        Map<String, Object> clientProps = new HashMap();
        clientProps.put("bootstrap.servers", "localhost:9092");

        AdminClient adminClient = AdminClient.create(clientProps);

        try {
            DescribeConfigsOptions options = new DescribeConfigsOptions()
                    .includeDocumentation(true);
            Collection<ConfigResource> resources = Arrays.asList(
                    new ConfigResource(ConfigResource.Type.BROKER_LOGGER,"0")
            );
            DescribeConfigsResult describeConfigs = adminClient.describeConfigs(resources, options);
            Map<ConfigResource, Config> map = describeConfigs.all().get();
            for (Map.Entry<ConfigResource, Config> entry : map.entrySet()) {
                for (ConfigEntry configEntry : entry.getValue().entries()) {
                    System.err.println(configEntry.name() + " : " + configEntry.value());
                }
            }

            Thread.sleep(10000L);

            // incremental alter BROKER_LOGGER resource
            Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
            ConfigEntry entry = new ConfigEntry("kafka.server.KafkaApis", "DEBUG");
            configs.put(new ConfigResource(ConfigResource.Type.BROKER_LOGGER, "0"),
                    Collections.singletonList(new AlterConfigOp(entry, AlterConfigOp.OpType.SET)));
            AlterConfigsResult acr = adminClient.incrementalAlterConfigs(configs);
            acr.all().get();

        } finally {
            Thread.sleep(2000L);
        }

    }
}
