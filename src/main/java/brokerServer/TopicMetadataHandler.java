package brokerServer;


import io.netty.channel.ChannelHandlerContext;
import model.AckData;
import model.ProducerRecord;
import model.Topic;
import model.Topics;
import model.response.ResponseTopicMetadata;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.log4j.Logger;
import util.AvroSerializers;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TopicMetadataHandler extends AvroSerializers {
    private final Path defaultPath;
    private static final String TOPIC_LIST = "topicList";
    private final Logger logger = Logger.getLogger(TopicMetadataHandler.class);
    private final ExecutorService executorService;
    private final Properties properties;

    public TopicMetadataHandler(Properties properties) {
        int ioThread = Integer.parseInt(properties.getProperty(BrokerConfig.IO_THREAD.getValue()));
        executorService = Executors.newFixedThreadPool(ioThread);
        this.defaultPath = Path.of(properties.getProperty(BrokerConfig.LOG_DIRS.getValue()));
        this.properties = properties;
    }

    //broker server가 실행되자마자 호출
    public void getTopicMetaData(BrokerServer.TopicsListener topicsListener) throws Exception {
        Path path = Path.of(defaultPath + "/" + TOPIC_LIST);

        if (!Files.exists(path)) {
            Files.createFile(path);
            logger.info("topicList file 생성 완료했습니다. 현재 topicList는 0개입니다.");
        }

        AsynchronousFileChannel asynchronousFileChannel = AsynchronousFileChannel.open(path, EnumSet.of(StandardOpenOption.READ), executorService);
        ByteBuffer buffer = ByteBuffer.allocate((int) asynchronousFileChannel.size());

        asynchronousFileChannel.read(buffer, 0, null, new CompletionHandler<Integer, Object>() {
            @Override
            public void completed(Integer result, Object attachment) {

                if (result == -1) {
                    logger.error("파일을 읽어오는데서 문제가 발생했습니다. result =" + result);
                    return;
                }

                Schema schema = ReflectData.get().getSchema(Topics.class);

                try {
                    Topics topics = (Topics) getDeserialization(buffer.array(), schema);
                    topicsListener.setTopics(topics);

                    logger.info("topic info: " + DataRepository.getInstance().getTopics());

                } catch (EOFException e) {
                    logger.info("topicList에 존재하는 topic이 없습니다.");

                } catch (Exception e) {
                    logger.error(e);
                }
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                logger.error("topic metaData를 가져오던 중 문제가 발생했습니다", exc);
            }
        });
    }

    public void createTopic(ChannelHandlerContext ctx, ProducerRecord record) throws Exception {

        String autoCreateTopic = properties.getProperty(BrokerConfig.AUTO_CREATE_TOPIC.getValue());
        int partitions = Integer.parseInt(properties.getProperty(BrokerConfig.TOPIC_PARTITIONS.getValue()));
        int replicationFactor = Integer.parseInt(properties.getProperty(BrokerConfig.REPLICATION_FACTOR.getValue()));
        String brokerID = properties.getProperty(BrokerConfig.ID.getValue());
        Schema schema = ReflectData.get().getSchema(Topics.class);

        if (autoCreateTopic.equals("false")) {
            ctx.channel().writeAndFlush(new AckData(500, "\"" + record.getTopic() + "\"" + "은 broker에 존재하지 않습니다."));
            return;
        }

        //todo controller에 연결된 브로커 개수에 따라서 replication-factor가 정해질 수 있어야 한다
//      if ( replicationFactor > 현재 controller와 연결된 broker들의 개수) {
//                logger.error("현재 controller에 연결된 broker들의 수보다 replication-factor 값이 더 큽니다");
//                return;
//            }

        //todo partition의 leader가 될 broker들을 선정하는 로직이 필요하다. 마찬가지로 controller 구현 후 작업해야 할 것 같다.
//      int brokerLeader =  elect brokerLeader using Round robin fashion

        Path path = Path.of(defaultPath + "/" + TOPIC_LIST);

        if (!Files.exists(path)) {
            Files.createFile(path);
            logger.info(TOPIC_LIST + " file 생성 완료");
        }

        Topic newTopicMetadata = new Topic(record.getTopic(), partitions, "0", 0);

        DataRepository.getInstance().addTopic(newTopicMetadata);

        byte[] bytes = getSerialization(DataRepository.getInstance().getTopics(), schema);

        ByteBuffer serialized = ByteBuffer.allocate(bytes.length);
        serialized.put(bytes);

        serialized.flip();

        AsynchronousFileChannel asynchronousFileChannel = AsynchronousFileChannel.open(path, EnumSet.of(StandardOpenOption.WRITE), executorService);

        asynchronousFileChannel.write(serialized, 0, null, new CompletionHandler<Integer, Object>() {
            @Override
            public void completed(Integer result, Object attachment) {

                if (result == -1) {
                    logger.error("파일을 작성하면서 문제가 발생했습니다.");
                    return;
                }
                logger.info("성공적으로 토픽을 생성했습니다.");
                ctx.channel().writeAndFlush(new ResponseTopicMetadata(record, newTopicMetadata));
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                logger.error("토픽 생성을 실행하던 중, 문제가 발생했습니다", exc);
            }
        });
    }
}
