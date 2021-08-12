package consumer;


import java.util.Collection;
import java.util.HashMap;

public class ConsumerManager {
    // consumerid-consumerClient
    private final HashMap<String, ConsumerClient> consumerMap;

    public ConsumerManager() {
        consumerMap = new HashMap<>();
    }

    public static ConsumerManager getInstance() {
        return SingletonConsumerManager.INSTANCE;
    }

    public ConsumerClient getConsumer(String consumerId) {
        return consumerMap.get(consumerId);
    }

    public void addConsumer(String consumerId, ConsumerClient consumer) {
        consumerMap.put(consumerId, consumer);
    }

    public void subscribe(Collection<String> topics, String consumerId) {
        ConsumerClient consumer = consumerMap.get(consumerId);
        consumer.subscribe(topics);
        consumerMap.put(consumerId, consumer);
    }

    public void poll(long timeout, String consumerId) {
        ConsumerClient consumer = consumerMap.get(consumerId);

        if (!consumer.checkSubscription()) {
            throw new NullPointerException("구독한 토픽이 존재하지 않습니다.");
        } else {
            consumer.poll();
        }
    }


    private static class SingletonConsumerManager {
        private static final ConsumerManager INSTANCE = new ConsumerManager();
    }
}
