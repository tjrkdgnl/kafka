package consumer;


import model.TopicPartition;

import java.util.*;

public class SubscribeState {

    private Set<String> subscriptions;

    //토픽에 대한 offset을 관리
    private final HashMap<TopicPartition,Integer> topicPartitionAndOffset;

    public SubscribeState(){
        topicPartitionAndOffset = new HashMap<>();
    }

    public HashMap<TopicPartition, Integer> getTopicPartitionAndOffset() {
        return topicPartitionAndOffset;
    }

    public void setSubscriptions(Set<String> topics){
        subscriptions = topics;
    }

    public String[] getSubscriptionTopics(){
        return subscriptions.toArray(new String[subscriptions.size()-1]);
    }

    public void setTopicPartition(TopicPartition topicPartition, int offset) {
        topicPartitionAndOffset.put(topicPartition,offset);
    }
}
