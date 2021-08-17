package consumer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import manager.NetworkManager;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HeartbeatClient {
    private final Logger logger;
    private ChannelFuture channelFuture;
    private final ScheduledExecutorService executorService;
    private final String groupId;
    private final String consumerId;
    private final int sessionTimeout;
    private final int heartbeatInterval;

    public HeartbeatClient(Properties properties) {
        logger = Logger.getLogger(HeartbeatClient.class);
        executorService = Executors.newScheduledThreadPool(1);
        groupId = properties.getProperty(ConsumerConfig.GROUP_ID.name());
        consumerId = properties.getProperty(ConsumerConfig.CONSUMER_ID.name());
        sessionTimeout = Integer.parseInt(properties.getProperty(ConsumerConfig.SESSION_TIMEOUT.getValue(), "10000"));
        heartbeatInterval = Integer.parseInt(properties.getProperty(ConsumerConfig.HEARTBEAT_INTERVAL.getValue(), "3000"));

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
                        socketChannel.pipeline().addLast(new HeartbeatOutBoundHandler());
                    }
                });

        channelFuture = bootstrap.connect().sync();

    }

    public void wakeUpHeartbeat() {
        Heartbeat heartbeat = new Heartbeat(channelFuture, groupId, consumerId, sessionTimeout);
        executorService.scheduleAtFixedRate(heartbeat, 0, heartbeatInterval, TimeUnit.MILLISECONDS);
    }


}
