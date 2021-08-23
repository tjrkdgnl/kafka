package brokerServer;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import manager.NetworkManager;
import model.Topic;
import model.TopicPartition;
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
    private final ScheduledExecutorService executorService;

    public BrokerServer(Properties brokerProperties) throws Exception {
        properties = brokerProperties;

        this.host = properties.getProperty(BrokerConfig.HOST.getValue());
        this.port = Integer.parseInt(properties.getProperty(BrokerConfig.PORT.getValue()));
        this.executorService = Executors.newScheduledThreadPool(1);
        ConsumerGroupOffsetHandler consumerGroupOffsetHandler = new ConsumerGroupOffsetHandler(properties);
        ConsumerRecordsHandler consumerRecordsHandler = new ConsumerRecordsHandler(properties);
        TopicMetadataHandler topicMetadataHandler = new TopicMetadataHandler(brokerProperties);

        consumerGroupOffsetHandler.readConsumersOffset(consumersOffset -> {
            if (consumersOffset != null) {
                DataRepository.getInstance().updateConsumersOffsetMap(consumersOffset.getConsumerOffsetMap());
                logger.info("set Consumer__offsets :" + DataRepository.getInstance().getConsumerOffsetMap());
            }
        });

        //broker server가 실행되면 topic list 정보를 얻어온다
        topicMetadataHandler.getTopicMetaData(topics -> {
            DataRepository.getInstance().setTopics(topics);

            //모든 레코드들을 불러와서 레포지토리에 셋팅한다
            for (Topic topic : topics.getTopicList()) {
                for (int partition = 0; partition < topic.getPartitions(); partition++) {
                    TopicPartition topicPartition = new TopicPartition(topic.getTopic(), partition);

                    consumerRecordsHandler.readRecords(topicPartition, records -> {
                        DataRepository.getInstance().setRecords(topicPartition, records);
                        logger.info("set Records :" + DataRepository.getInstance().getRecords(topicPartition));
                    });
                }
            }
        });
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

    public static Properties getProperties() {
        return BrokerRepository.PROPERTIES;
    }

    private static class BrokerRepository {
        private static final Properties PROPERTIES = BrokerServer.properties;
    }

    @FunctionalInterface
    public interface TopicsListener {
        void setTopics(Topics topics);
    }

}

