package consumer;

import model.TopicPartition;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SubscribeState {

    private final Set<String> subscriptions;

    private final Set<TopicPartition> assignedTopicWithPartition;

    public SubscribeState(){
        assignedTopicWithPartition = new HashSet<>();
        subscriptions = new HashSet<>();
    }

    public void setAssignedTopicWithPartition(TopicPartition topicPartition){
        this.assignedTopicWithPartition.add(topicPartition);
    }

    public void setSubscriptions(Set<String> topics){
        subscriptions.addAll(topics);
    }

    public List<TopicPartition> getSubscriptions(){
        ArrayList<TopicPartition> topicPartitionArrayList =new ArrayList<>();

        if(assignedTopicWithPartition.size() != 0){
            topicPartitionArrayList.addAll(assignedTopicWithPartition);
        }

        for(String topic : subscriptions){
            TopicPartition topicPartition =new TopicPartition(topic,-1);

            if(!topicPartitionArrayList.contains(topicPartition)){
                topicPartitionArrayList.add(topicPartition);
            }
        }

        return topicPartitionArrayList;
    }

}
