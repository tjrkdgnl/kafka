package util;


import brokerServer.BrokerConfig;
import io.netty.channel.ChannelHandlerContext;
import model.AckData;
import model.ProducerRecord;
import model.Topic;
import model.Topics;
import model.response.ResponseTopicMetadata;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.log4j.Logger;
import java.io.EOFException;
import java.io.File;
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
    private final ExecutorService executorService = Executors.newFixedThreadPool(3);

    public TopicMetadataHandler(Path path) {
        this.defaultPath = path;
    }

    public void getTopicMetaData(ChannelHandlerContext ctx, ProducerRecord record, Properties properties) throws Exception {
        ctx.executor().execute(() -> {
            try {
                Path path = Path.of(defaultPath + "/" + TOPIC_LIST);

                if (!Files.exists(path)) {
                    Files.createFile(path);
                    logger.info("topicList file 생성 완료했습니다. 현재 topicList는 0개입니다.");
                }

                AsynchronousFileChannel asynchronousFileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.READ);
                ByteBuffer buffer = ByteBuffer.allocate((int) asynchronousFileChannel.size());

                asynchronousFileChannel.read(buffer, 0, null, new CompletionHandler<Integer, Object>() {
                    @Override
                    public void completed(Integer result, Object attachment) {

                        if (result == -1) {
                            logger.error("파일을 읽어오는데서 문제가 발생했습니다. result ="+ result);
                            return;
                        }

                        String autoCreateTopic = properties.getProperty(BrokerConfig.AUTO_CREATE_TOPIC.getValue());
                        int partitions = Integer.parseInt(properties.getProperty(BrokerConfig.TOPIC_PARTITIONS.getValue()));
                        int replicationFactor = Integer.parseInt(properties.getProperty(BrokerConfig.REPLICATION_FACTOR.getValue()));

                        Schema schema = ReflectData.get().getSchema(Topics.class);

                        try {
                            Topics topics = (Topics) getDeserialization(buffer.array(), schema);

                            //요청한 토픽이 존재하는지 체크
                            for (Topic topicMetadata : topics.getTopicList()) {
                                if (topicMetadata.getTopic().equals(record.getTopic())) {
                                    ctx.channel().writeAndFlush(new ResponseTopicMetadata(topicMetadata));
                                    return;
                                }
                            }

                            //topics는 존재하지만 Producer Record로 들어온 topic은 존재하지 않는 경우 토픽 생성
                            createTopic(ctx, schema, record, topics, partitions, replicationFactor, autoCreateTopic);

                        } catch (EOFException e) {
                            logger.info("작성된 topicList가 존재하지 않습니다. ");

                            Topics topics = new Topics();

                            //만약 요청한 토픽이 topics가 한 개도 존재하지 않으면 topic을 생성하고 client에게 전송한다
                            try {
                                createTopic(ctx, schema, record, topics, partitions, replicationFactor, autoCreateTopic);

                            } catch (Exception exception) {
                                logger.error( exception);
                            }

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
            catch (Exception e) {
                logger.error(e);
            }
        });
    }

    public void createTopic(ChannelHandlerContext ctx, Schema schema, ProducerRecord record, Topics topics,
                            int partitions, int replicationFactor, String autoCreateTopic) throws Exception {

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

        File file = new File(path.toString());

        Topic newTopicMetadata = new Topic(record.getTopic(), partitions, "0", 0);

        topics.getTopicList().add(newTopicMetadata);

        byte[] bytes = getSerialization(topics, schema);

        ByteBuffer serialized = ByteBuffer.allocate(bytes.length);
        serialized.put(bytes);

        serialized.flip();

        AsynchronousFileChannel asynchronousFileChannel = AsynchronousFileChannel.open(path, EnumSet.of(StandardOpenOption.WRITE), executorService);

        asynchronousFileChannel.write(serialized, asynchronousFileChannel.size(), null, new CompletionHandler<Integer, Object>() {
            @Override
            public void completed(Integer result, Object attachment) {

                if (result == -1) {
                    logger.error("파일을 작성하면서 문제가 발생했습니다.");
                    return;
                }

                ctx.channel().writeAndFlush(new ResponseTopicMetadata(newTopicMetadata));
                logger.info("topic 생성 완료 | file size: " + file.length() + " bytes");
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                logger.error("토픽 생성을 실행하던 중, 문제가 발생했습니다", exc);
            }
        });

    }
}
