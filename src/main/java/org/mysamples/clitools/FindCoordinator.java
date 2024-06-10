package org.mysamples.clitools;

import org.apache.kafka.common.utils.Utils;

public class FindCoordinator {


    public static void main(String[] args) {
        String groupId = "";
        System.out.println("Partition for group '" + groupId +"' = " + partitionForGroup(groupId));
        groupId = "q219j2rn82rbzh8v";
        System.out.println("Partition for group '" + groupId +"' = " + partitionForGroup(groupId));
        groupId = "qp76g99g85x7pzcw";
        System.out.println("Partition for group '" + groupId +"' = " + partitionForGroup(groupId));
        groupId = "npv7zwpvqmjpzjs5";
        System.out.println("Partition for group '" + groupId +"' = " + partitionForGroup(groupId));
    }

    private static int partitionForGroup(String groupId) {
        int groupMetadataTopicPartitionCount = 50;
        int partitionForGroup = Utils.abs(groupId.hashCode()) % groupMetadataTopicPartitionCount;
        return partitionForGroup;
    }
}
