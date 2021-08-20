package consumer;


import model.ConsumerRecord;
import model.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class ConsumerManager {
    // consumerid-consumerClient
    private final HashMap<String, ConsumerClient> consumerMap;
    //consumerId-List<ConsumerRecord>
    private final HashMap<String, List<ConsumerRecord>> consumerRecordsMap;


    public ConsumerManager() {
        consumerMap = new HashMap<>();
        consumerRecordsMap = new HashMap<>();
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

    public void assign(List<TopicPartition> topicPartitions, String consumerId) {
        ConsumerClient consumer = consumerMap.get(consumerId);
        consumer.assign(topicPartitions);
    }

    public void subscribe(Collection<String> topics, String consumerId) {
        ConsumerClient consumer = consumerMap.get(consumerId);
        consumer.subscribe(topics);
        consumerMap.put(consumerId, consumer);
    }

    public void poll(String consumerId) {
        ConsumerClient consumer = consumerMap.get(consumerId);

        if (!consumer.checkSubscription()) {
            throw new IllegalStateException("구독한 토픽이 존재하지 않습니다.");
        } else {
            consumer.wakeUpHeartbeat();

            consumer.poll();
        }
    }


    public void setConsumerRecords(String consumerId, List<ConsumerRecord> consumerRecords) {
        consumerRecordsMap.put(consumerId, consumerRecords);
    }

    public List<ConsumerRecord> getConsumerRecord(String consumerId) {
        return consumerRecordsMap.containsKey(consumerId) ? consumerRecordsMap.remove(consumerId) : new ArrayList<>();
    }

    private static class SingletonConsumerManager {
        private static final ConsumerManager INSTANCE = new ConsumerManager();
    }

}
