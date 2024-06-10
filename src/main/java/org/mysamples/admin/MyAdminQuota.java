package org.mysamples.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClientQuotasResult;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.log4j.Level;
import org.mysamples.common.Common;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MyAdminQuota {

    public static void main(String[] args) throws Exception {
        Common.log4jClientConfig(Level.INFO);

       Map clientProps = new HashMap<>();
       clientProps.put("bootstrap.servers", "localhost:9092");

        try (AdminClient adminClient = AdminClient.create(clientProps)) {
            // LIST TOPIC
            Set<String> topicNames = adminClient.listTopics().names().get();
            System.out.println(">>>listTopics: count=" + topicNames.size() + "\n" + topicNames);
            System.out.println();

//            String iamID = "iam-ServiceId-aa7f5ff4-0118-41dd-aed5-4995a0b9d021"; //set iam id of target user to set quotas to
            // if null is used instead of the iamID string, the following quota alteration will be applied to the default user
            String iamID = null;

            // describe quotas
            System.out.println(">>>describeQuotas:");
            DescribeClientQuotasResult describeClientQuotasFuture = adminClient.describeClientQuotas(ClientQuotaFilter.all());
            System.out.println(describeClientQuotasFuture.entities().get());

            // add quotas
            if (false) {
                System.out.println(">>>addQuotas:");
                ClientQuotaEntity entity = new ClientQuotaEntity(
                        Collections.singletonMap(ClientQuotaEntity.USER, iamID));
                ClientQuotaAlteration alteration = new ClientQuotaAlteration(entity,
                        Arrays.asList(new ClientQuotaAlteration.Op("consumer_byte_rate", 1000.0),
                                new ClientQuotaAlteration.Op("producer_byte_rate", 1000.0)));
                adminClient.alterClientQuotas(Arrays.asList(alteration)).all().get();
                Thread.sleep(1000);
            }

            System.out.println(">>>describeQuotas:");
            describeClientQuotasFuture = adminClient.describeClientQuotas(ClientQuotaFilter.all());
            System.out.println(describeClientQuotasFuture.entities().get());

            //remove quotas (set them to null)
            if (false) {
                ClientQuotaEntity entity = new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.USER, iamID));
                ClientQuotaAlteration alteration = new ClientQuotaAlteration(entity,
                        Arrays.asList(new ClientQuotaAlteration.Op("consumer_byte_rate", null),
                                new ClientQuotaAlteration.Op("producer_byte_rate", null)));
                adminClient.alterClientQuotas(Arrays.asList(alteration)).all().get();
                Thread.sleep(1000);
            }

            System.out.println(">>>describeQuotas:");
            describeClientQuotasFuture = adminClient.describeClientQuotas(ClientQuotaFilter.all());
            System.out.println(describeClientQuotasFuture.entities().get());

        } finally {
            Thread.sleep(2000L);
        }
    }
}

