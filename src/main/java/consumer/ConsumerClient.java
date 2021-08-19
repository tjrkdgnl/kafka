package consumer;

import io.netty.channel.ChannelFuture;
import model.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class ConsumerClient {
    private final SubscribeState subscribeState;
    private final ConsumerMetadata metadata;
    private final Fetcher fetcher;
    private final HeartbeatClient heartbeatClient;

    public ConsumerClient(Properties properties, ChannelFuture channelFuture, String groupId, String consumerId) {
        subscribeState = new SubscribeState();
        metadata = new ConsumerMetadata();
        fetcher = new Fetcher(metadata, subscribeState, channelFuture, groupId, consumerId);
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

}
