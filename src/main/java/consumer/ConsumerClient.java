package consumer;

import io.netty.channel.ChannelFuture;
import model.TopicPartition;

import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;

public class ConsumerClient {
    private final Properties properties;
    private final SubscribeState subscribeState;
    private final ConsumerMetadata metadata;
    private final Fetcher fetcher;


    public ConsumerClient(Properties properties, ChannelFuture channelFuture, String groupId, String consumerId) {
        subscribeState = new SubscribeState();
        metadata = new ConsumerMetadata();
        this.properties = properties;
        fetcher = new Fetcher(metadata, subscribeState, channelFuture, groupId, consumerId);
    }

    public void assign(TopicPartition topicPartition) {
        this.subscribeState.setAssignedTopicWithPartition(topicPartition);
    }

    public void subscribe(Collection<String> topics) {
        this.subscribeState.setSubscriptions(new HashSet<>(topics));
    }

    public boolean checkSubscription() {
        if (this.subscribeState.getSubscriptions() == null) {
            return false;
        } else if(this.subscribeState.getSubscriptions().isEmpty()){
            return false;
        } else {
            return true;
        }
    }

    //추후에 ConsumerRecords 리턴하도록 구현하기
    public void poll() {
        this.fetcher.pollForFetches();
    }

    public Fetcher getFetcher() {
        return this.fetcher;
    }

}
