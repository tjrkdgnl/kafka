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

import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class KafkaConsumer {
    private final Logger logger = Logger.getLogger(KafkaConsumer.class);
    private Properties properties;
    private String groupId;
    private String consumerId;

    public KafkaConsumer(Properties properties) {

        try {
            groupId = properties.getProperty(ConsumerConfig.GROUP_ID.name());
            consumerId = properties.getProperty(ConsumerConfig.CONSUMER_ID.name());
            this.properties = properties;
            start();
        } catch (Exception e) {
            logger.info("server와 연결 중 문제가 발생했습니다.", e);
            System.exit(-1);
        }
    }

    public void start() throws Exception {
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();

        String[] address = properties.getProperty(ConsumerConfig.SERVER.name()).split(":");

        String host = address[0];
        int port = Integer.parseInt(address[1].trim());

        logger.info("host: " + host + " port: " + port);

        Bootstrap bootstrap = NetworkManager.getInstance().buildClient(eventLoopGroup, host, port)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) {
                        socketChannel.pipeline().addLast(new ConsumerInBoundHandler());
                        socketChannel.pipeline().addLast(new ConsumerOutBoundHandler());
                    }
                });

        ChannelFuture channelFuture = bootstrap.connect().sync();

        ConsumerClient consumer = new ConsumerClient(properties, channelFuture, groupId, consumerId);
        ConsumerManager.getInstance().addConsumer(consumerId, consumer);
    }

    public void assign(List<TopicPartition> topicPartitions) {
        ConsumerManager.getInstance().assign(topicPartitions, consumerId);
    }

    public void subscribe(Collection<String> topics) {
        ConsumerManager.getInstance().subscribe(topics, consumerId);
    }


    public void poll() throws InterruptedException {

        ConsumerManager.getInstance().poll(consumerId);
        Thread.sleep(5000);
    }
}
