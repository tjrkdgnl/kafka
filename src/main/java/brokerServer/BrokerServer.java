package brokerServer;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import manager.NetworkManager;
import model.Topics;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BrokerServer {
    private final Logger logger = Logger.getLogger(BrokerServer.class);
    private final String host;
    private final int port;
    private static Properties properties;
    public static Topics topics;
    private final ScheduledExecutorService executorService;

    public BrokerServer(Properties brokerProperties) throws Exception {
        properties = brokerProperties;

        this.host = properties.getProperty(BrokerConfig.HOST.getValue());
        this.port = Integer.parseInt(properties.getProperty(BrokerConfig.PORT.getValue()));
        this.executorService = Executors.newScheduledThreadPool(1);

        //broker server가 실행되면 topic list 정보를 얻어온다
        BrokerRepository.TOPIC_METADATA_HANDLER.getTopicMetaData();
    }

    public void start() {
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        EventLoopGroup workerEventLoopGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap bootstrap = NetworkManager.getInstance().buildServer(eventLoopGroup, workerEventLoopGroup, host, port);
            bootstrap.bind().sync();

            HeartbeatScheduler heartbeatScheduler = new HeartbeatScheduler(properties);
            int heartBeatInterval = Integer.parseInt(properties.getProperty(BrokerConfig.HEARTBEAT_INTERVAL.getValue(), "6000"));
            executorService.scheduleAtFixedRate(heartbeatScheduler, 0, heartBeatInterval, TimeUnit.MILLISECONDS);

        } catch (Exception e) {
            logger.error("broker server를 실행하던 중 문제가 발생했습니다.", e);
            System.exit(-1);
        }
    }

    public static TopicMetadataHandler getTopicMetadataHandler() {
        return BrokerRepository.TOPIC_METADATA_HANDLER;
    }

    public static Properties getProperties() {
        return BrokerRepository.PROPERTIES;
    }

    private static class BrokerRepository {
        private static final Properties PROPERTIES = BrokerServer.properties;
        private static final TopicMetadataHandler TOPIC_METADATA_HANDLER = new TopicMetadataHandler(PROPERTIES);
    }


}
