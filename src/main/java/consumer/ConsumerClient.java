package consumer;

import io.netty.channel.ChannelFuture;
import model.ConsumerOffsetInfo;
import model.TopicPartition;

import java.util.*;

public class ConsumerClient {
    private final SubscribeState subscribeState;
    private final ConsumerMetadata metadata;
    private final Fetcher fetcher;
    private final HeartbeatClient heartbeatClient;

    public ConsumerClient(Properties properties, ChannelFuture channelFuture, String groupId, String consumerId) {
        subscribeState = new SubscribeState();
        metadata = new ConsumerMetadata();
        int recordSize = Integer.parseInt(properties.getProperty(ConsumerConfig.MAX_POLL_RECORDS.name(), "1"));
        fetcher = new Fetcher(properties, metadata, subscribeState, channelFuture, groupId, consumerId, recordSize);
        heartbeatClient = new HeartbeatClient(properties);
    }

    public void assign(List<TopicPartition> topicPartition) {
        this.subscribeState.setSubscriptions(topicPartition);
    }

    public void subscribe(Collection<String> topics) {
        List<TopicPartition> topicPartitions = new ArrayList<>();

        for (String topic : topics) {
            topicPartitions.add(new TopicPartition(topic, -1));
        }

        this.subscribeState.setSubscriptions(topicPartitions);
    }


    public boolean checkSubscription() {
        if (this.subscribeState.getSubscriptions() == null || this.subscribeState.getSubscriptions().isEmpty()) {
            return false;
        } else {
            return true;
        }
    }

    public void wakeUpHeartbeat() {
        heartbeatClient.wakeUpHeartbeat();
    }

    //추후에 ConsumerRecords 리턴하도록 구현하기
    public void poll() {
        this.fetcher.pollForFetches();
    }

    public Fetcher getFetcher() {
        return this.fetcher;
    }

    public void setUnCommitedOffset(HashMap<ConsumerOffsetInfo, Integer> unCommitedOffset) {
        this.fetcher.setUnCommitedOffsetInfo(unCommitedOffset);
    }

    public void updateOffset() {
        this.metadata.updateTopicPartitionAndOffset();
    }

    public int getOffset(TopicPartition topicPartition) {
        return this.metadata.getOffset(topicPartition);
    }

}
