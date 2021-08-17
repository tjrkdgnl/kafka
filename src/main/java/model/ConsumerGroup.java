package model;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

public class ConsumerGroup implements Serializable {
    private String groupId;
    private int rebalanceId;


    //member가 갖고 있는 topic list
    private HashMap<String, List<TopicPartition>> ownershipMap;

    //topic을 구독하고 있는 consumer list
    private final HashMap<String, List<String>> topicMap;

    public ConsumerGroup() {
        ownershipMap = new HashMap<>();
        topicMap = new HashMap<>();
        rebalanceId = 0;
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
        this.ownershipMap = new HashMap<>();
    }

    public void addOwnership(String consumer, List<TopicPartition> topicPartitions) {
        ownershipMap.put(consumer, topicPartitions);
    }

    public HashMap<String, List<TopicPartition>> getOwnershipMap() {
        return new HashMap<>(ownershipMap);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
    }
}
