package consumer;

import model.TopicPartition;

import java.util.HashSet;
import java.util.Set;

public class SubscribeState {

    private Set<String> subscriptions;

    private final Set<TopicPartition> assignedTopicWithPartition;

    public SubscribeState() {
        assignedTopicWithPartition = new HashSet<>();
    }

    public void setAssignedTopicWithPartition(TopicPartition topicPartition) {
        this.assignedTopicWithPartition.add(topicPartition);
    }

    public void setSubscriptions(Set<String> topics) {
        subscriptions.addAll(topics);
    }

    public Set<String> getSubscriptions() {
        return subscriptions;
    }

    public String[] getTopics() {
        return subscriptions.toArray(new String[subscriptions.size() - 1]);
    }

}
