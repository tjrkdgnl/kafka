package consumer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import manager.NetworkManager;
import model.TopicPartition;
import model.request.RequestMessage;
import org.apache.log4j.Logger;
import util.MemberState;

import java.util.List;
import java.util.Properties;

public class ConsumerRecordClient {
    private Logger logger;
    private ChannelFuture channelFuture;

    public ConsumerRecordClient(Properties properties) {
        logger = Logger.getLogger(ConsumerRecordClient.class);


        try {
            String[] address = properties.getProperty(ConsumerConfig.SERVER.name()).split(":");
            String host = address[0];
            int port = Integer.parseInt(address[1].trim());
            start(host, port);


        } catch (Exception e) {
            logger.error("heartbeatClient가 서버에 연결하던 중 문제가 생겼습니다. ", e);
        }

    }

    private void start(String host, int port) throws Exception {
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        Bootstrap bootstrap = NetworkManager.getInstance().buildClient(eventLoopGroup, host, port)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        socketChannel.pipeline().addLast(new ConsumerRecordInBoundHandler());
                        socketChannel.pipeline().addLast(new ConsumerOutBoundHandler());
                    }
                });

        channelFuture = bootstrap.connect().sync();
    }

    public void requestConsumerRecords(MemberState status, int rebalanceId, List<TopicPartition> subscriptions,
                                       String consumerId, String groupId, int recordSize) {
        channelFuture.channel().writeAndFlush(new RequestMessage(status, rebalanceId,
                subscriptions, consumerId, groupId, recordSize));
    }


}
