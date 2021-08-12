package consumer;

import io.netty.channel.ChannelFuture;
import model.TopicPartition;

import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;

public class ConsumerClient {
    public static Properties properties;
    private SubscribeState subscribeState;
    public ConsumerMetadata metadata;
    private Fetcher fetcher;

    public ConsumerClient() {

    }

    public void init(Properties properties, ChannelFuture channelFuture) throws Exception {
        subscribeState = new SubscribeState();
        metadata = new ConsumerMetadata();
        ConsumerClient.properties = properties;
        String groupId = properties.getProperty(ConsumerConfig.GROUP_ID.name());
        String consumerId = properties.getProperty(ConsumerConfig.CONSUMER_ID.name());
        fetcher = new Fetcher(metadata, subscribeState, channelFuture, groupId, consumerId);
    }

    public static ConsumerClient getInstance() {
        return SingleTonKafkaConsumerClient.INSTANCE;
    }

    public void assign(TopicPartition topicPartition) {
        this.subscribeState.setAssignedTopicWithPartition(topicPartition);
    }

    public void subscribe(Collection<String> topics) {
        if (topics == null) {
            throw new NullPointerException("Topics Collections is null ");
        } else {
            this.subscribeState.setSubscriptions(new HashSet<>(topics));
        }
    }


    //추후에 ConsumerRecords 리턴하도록 구현하기
    public void poll() {
        this.fetcher.pollForFetches();
    }

    public Fetcher getFetcher(){
        return fetcher;
    }

    private static class SingleTonKafkaConsumerClient {
        private static final ConsumerClient INSTANCE = new ConsumerClient();
    }
}
