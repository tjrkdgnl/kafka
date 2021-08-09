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
import util.DataUtil;

import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class KafkaConsumer {
    private final Logger logger = Logger.getLogger(KafkaConsumer.class);
    public static Properties properties;
    private final SubscribeState subscribeState;
    public ConsumerMetadata metadata;
    private ChannelFuture channelFuture;
    private final ConsumerCoordinator consumerCoordinator;
    private final String group_id;
    private final String consumer_id;
    private final Fetcher fetcher;

    public KafkaConsumer(Properties properties) throws Exception {
        subscribeState = new SubscribeState();
        metadata = new ConsumerMetadata();
        consumerCoordinator = new ConsumerCoordinator(metadata, subscribeState);
        KafkaConsumer.properties = properties;
        group_id = properties.getProperty(ConsumerConfig.GROUP_ID.name());
        consumer_id = ("Consumer-" + DataUtil.createTimestamp()).trim();
        fetcher = new Fetcher(metadata);

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

    }

    public void assign(TopicPartition topicPartition) {
        this.subscribeState.setAssignedTopicWithPartition(topicPartition);
    }

    public void subscribe(Collection<String> topics) {
        if (topics == null) {
            throw new NullPointerException("Topics Collections is null ");
        } else if (topics.isEmpty()) {
            this.unSubscribe();
        } else {
            this.subscribeState.setSubscriptions(new HashSet<>(topics));
        }
    }

    private void unSubscribe() {

    }

    //추후에 ConsumerRecords 리턴하도록 구현하기
    public void poll() {
        CompletableFuture<Boolean> joinFuture = new CompletableFuture<>();
        CompletableFuture<Boolean> updateFuture = new CompletableFuture<>();

        try {
            //polling 전, ownership 요청. ownership이 mapping 되기 전까지 대기
            this.consumerCoordinator.requestJoinGroup(joinFuture, channelFuture, group_id, consumer_id);

            boolean isJoin = joinFuture.get();

            if (isJoin) {
                this.fetcher.updateConsumerGroup(updateFuture, channelFuture, group_id);

                boolean isUpdate = updateFuture.get();

                if (isUpdate) {
                    //consumer Group 업데이트가 끝났다면 polling 시작
                    logger.info("Polling Start");
                } else {
                    logger.info("consumer group을 업데이트하지 못했습니다.");
                }

            } else {
                logger.info("consumerGroup에 가입하지 못했습니다. ");
            }

        } catch (Exception e) {
            logger.error("message 가져오는데 문제가 발생했습니다.", e);
        }
    }
}
