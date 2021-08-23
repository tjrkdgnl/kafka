package model;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConsumerGroup implements Serializable {
    private String groupId;
    private int rebalanceId;

    //member가 갖고 있는 topic list
    private HashMap<String, List<TopicPartition>> ownershipMap;
    //topic을 구독하고 있는 consumer list
    private final HashMap<String, List<String>> topicMap;
    //컨슈머로부터 직접 할당된 topicPartition
    private final HashMap<String, List<TopicPartition>> assignedOwnershipMap;


    public ConsumerGroup() {
        ownershipMap = new HashMap<>();
        topicMap = new HashMap<>();
        assignedOwnershipMap = new HashMap<>();
        rebalanceId = 0;
    }

    public boolean checkConsumer(String consumerId) {
        for (String consumer : ownershipMap.keySet()) {
            if (consumer.equals(consumerId)) {
                return true;
            }
        }
        return false;
    }

    public void addAssignedTopicPartition(String consumerId, TopicPartition topicPartition) {
        List<TopicPartition> list = assignedOwnershipMap.getOrDefault(consumerId, new ArrayList<>());

        for (Map.Entry<String, List<TopicPartition>> assignedList : assignedOwnershipMap.entrySet()) {
            String consumer = assignedList.getKey();
            List<TopicPartition> assignedTopicPartitions = assignedList.getValue();

            if (!consumer.equals(consumerId) && assignedTopicPartitions.contains(topicPartition)) {
                return;
            }
        }

        list.add(topicPartition);
        assignedOwnershipMap.put(consumerId, list);
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public int getRebalanceId() {
        return rebalanceId;
    }

    public void setRebalanceId(int rebalanceId) {
        this.rebalanceId = rebalanceId;
    }

    public void setConsumerList(String topic, List<String> consumerList) {
        this.topicMap.put(topic, consumerList);
    }

    public void updateTopicMap(String topic, List<String> currentConsumers) {
        this.topicMap.put(topic, currentConsumers);
    }

    public HashMap<String, List<String>> getTopicMap() {
        return new HashMap<>(topicMap);
    }

    public void initOwnership() {
        ownershipMap = new HashMap<>();

        for (String consumer : assignedOwnershipMap.keySet()) {
            for (List<TopicPartition> topicPartitions : assignedOwnershipMap.values()) {
                ownershipMap.put(consumer, new ArrayList<>(topicPartitions));
            }
        }
    }

    public void addOwnership(String consumer, TopicPartition topicPartition) {
        List<TopicPartition> topicPartitions = ownershipMap.getOrDefault(consumer, new ArrayList<>());

        if (!topicPartitions.contains(topicPartition)) {
            topicPartitions.add(topicPartition);
        }
        ownershipMap.put(consumer, topicPartitions);
    }

    public void removeOwnership(String consumerId) {
        ownershipMap.remove(consumerId);
    }

    public HashMap<String, List<TopicPartition>> getOwnershipMap() {
        return new HashMap<>(ownershipMap);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
    }
}
