package consumer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import manager.NetworkManager;
import model.TopicPartition;
import org.apache.log4j.Logger;
import util.ConsumerRequestStatus;
import util.DataUtil;

import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;

public class KafkaConsumer {
    private final Logger logger = Logger.getLogger(KafkaConsumer.class);
    public static Properties properties;
    private final SubscribeState subscribeState;
    public ConsumerMetadata metadata;
    private final Fetcher fetcher;
    private ChannelFuture channelFuture;

    public KafkaConsumer(Properties properties) throws Exception {
        subscribeState = new SubscribeState();
        metadata = new ConsumerMetadata();
        KafkaConsumer.properties = properties;
        String groupId = properties.getProperty(ConsumerConfig.GROUP_ID.name());
        String consumerId = properties.getProperty(ConsumerConfig.CONSUMER_ID.name());
        fetcher = new Fetcher(metadata, subscribeState, groupId, consumerId);

        start();
    }

    public void start() throws Exception {
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();

        String[] address = properties.getProperty(ConsumerConfig.SERVER.name()).split(":");

        String host = address[0];
        int port = Integer.parseInt(address[1].trim());

        logger.info("host: " + host + " port: " + port);

        Bootstrap bootstrap = NetworkManager.getInstance().createProducerClient(eventLoopGroup, host, port)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        socketChannel.pipeline().addLast(new ConsumerInBoundHandler());
                        socketChannel.pipeline().addLast(new ConsumerOutBoundHandler());
                    }
                });

         channelFuture = bootstrap.connect().sync();
        this.fetcher.setChannelFuture(channelFuture);

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
        if (this.fetcher.getStatus() == ConsumerRequestStatus.JOIN) {
            this.fetcher.joinConsumerGroup();
        }

    }
}
