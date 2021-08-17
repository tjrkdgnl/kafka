package consumer;

import model.TopicPartition;

import java.util.ArrayList;
import java.util.List;

public class SubscribeState {
    private List<TopicPartition> subscriptions;


    public void setSubscriptions(List<TopicPartition> topics) {
        if (subscriptions == null) {
            subscriptions = new ArrayList<>();
        }

        subscriptions.addAll(topics);
    }

    public List<TopicPartition> getSubscriptions() {
        return new ArrayList<>(subscriptions);
    }
}
