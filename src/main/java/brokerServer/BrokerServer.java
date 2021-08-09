package brokerServer;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import manager.NetworkManager;
import model.Topics;
import model.request.RequestJoinGroup;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class BrokerServer {
    private final Logger logger = Logger.getLogger(BrokerServer.class);
    private final String host;
    private final int port;
    public static Properties properties;
    public static TopicMetadataHandler topicMetadataHandler;
    public static ConsumerOwnershipHandler consumerOwnershipHandler;
    public static BlockingQueue<RequestJoinGroup> joinGroupQueue;
    public static Topics topics;

    public BrokerServer(Properties brokerProperties) throws Exception {
        properties = brokerProperties;

        this.host = properties.getProperty(BrokerConfig.HOST.getValue());
        this.port = Integer.parseInt(properties.getProperty(BrokerConfig.PORT.getValue()));
        topicMetadataHandler = new TopicMetadataHandler(properties);
        consumerOwnershipHandler = new ConsumerOwnershipHandler(properties);

        joinGroupQueue = new LinkedBlockingQueue<>();

        //broker server가 실행되면 topic list 정보를 얻어온다
        topicMetadataHandler.getTopicMetaData();
    }

    public void start() throws Exception {

        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        EventLoopGroup workerEventLoopGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap bootstrap = NetworkManager.getInstance().buildServer(eventLoopGroup, workerEventLoopGroup, host, port);

            bootstrap.bind().sync();

        } catch (Exception e) {
            logger.error("broker server를 실행하던 중 문제가 발생했습니다.", e);
        }
    }

}
