package brokerServer;

import io.netty.channel.ChannelHandlerContext;
import model.ConsumerGroup;
import model.request.RequestJoinGroup;
import model.request.UpdateConsumerGroup;
import model.response.ResponseError;
import model.response.ResponseGroupInfo;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.log4j.Logger;
import util.AvroSerializers;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerOwnershipHandler {
    private final Logger logger;
    private final AvroSerializers avroSerializers;
    private ChannelHandlerContext ctx;
    private final Path defaultPath;
    private Path groupPath;
    private RequestJoinGroup requestConsumerGroup;
    private final ExecutorService executorService;
    private final Schema consumerGroupsSchema;
    private CompletableFuture<ConsumerGroup> groupsCompletableFuture;
    private final GroupRebalanceHandler groupRebalanceHandler;

    public ConsumerOwnershipHandler(Properties properties) {
        this.logger = Logger.getLogger(ConsumerOwnershipHandler.class);
        this.avroSerializers = new AvroSerializers();
        this.defaultPath = Path.of(properties.getProperty(BrokerConfig.LOG_DIRS.getValue()));

        int ioThread = Integer.parseInt(properties.getProperty(BrokerConfig.IO_THREAD.getValue()));
        executorService = Executors.newFixedThreadPool(ioThread);

        consumerGroupsSchema = ReflectData.get().getSchema(ConsumerGroup.class);

        groupRebalanceHandler = new GroupRebalanceHandler();
    }


    public ConsumerOwnershipHandler init(ChannelHandlerContext ctx, RequestJoinGroup joinConsumerGroup) {
        this.ctx = ctx;
        this.requestConsumerGroup = joinConsumerGroup;

        return this;
    }


    public void joinConsumerGroup() {
        CompletableFuture.supplyAsync(() -> {
            try {
                groupPath = Path.of(defaultPath.toString() + "/" + requestConsumerGroup.getGroup_id());
                logger.info(groupPath);

                if (!Files.exists(groupPath)) {
                    Files.createFile(groupPath);
                    logger.info("Consumer Group 파일 생성 완료");
                }

                groupsCompletableFuture = new CompletableFuture<>();

                readGroupMetadata();

                return groupsCompletableFuture.get();
            } catch (Exception e) {
                logger.error("consumerGroup file을 읽던 중 문제가 발생했습니다", e);
                ctx.channel().writeAndFlush(new ResponseError(500, "consumer group file을 읽던 중 문제가 발생했습니다"));
            }
            return null;
        }).thenAcceptAsync(consumerGroup -> {
            try {
                writeGroupMetadata(consumerGroup);
            } catch (Exception e) {
                logger.error("consumerGroup file을 작성하던 중 문제가 발생했습니다", e);
                ctx.channel().writeAndFlush(new ResponseError(500, "consumer group file을 작성하던 중 문제가 발생했습니다"));
            }
        });
    }

    public void getConsumerGroup(String group_id){
        CompletableFuture.runAsync(()->{
            try {
                groupPath = Path.of(defaultPath.toString() + "/" + group_id);

                groupsCompletableFuture = new CompletableFuture<>();

                readGroupMetadata();

                ConsumerGroup consumerGroup = groupsCompletableFuture.get();

                ctx.channel().writeAndFlush(new ResponseGroupInfo(consumerGroup));

            }
            catch (Exception e){
                logger.error("Consumer group file을 읽던 중 문제가 발생했습니다",e);
                ctx.channel().writeAndFlush(new ResponseError(500,"Consumer group file을 읽던 중 문제가 발생했습니다"));
            }
        });
    }


    public void readGroupMetadata() throws Exception {
        //최근에 작성한 consumer group file 불러오기
        File file = new File(groupPath.toString());

        //ConsumerGroup 파일 읽기
        readAsyncFileChannel(file);
    }

    public void writeGroupMetadata(ConsumerGroup consumerGroup) throws Exception {
        //최근에 작성한 consumer file 불러오기
        File file = new File(groupPath.toString());

        writeAsyncFileChannel(file, consumerGroupsSchema, consumerGroup);
    }


    private void readAsyncFileChannel(File file) throws IOException {

        AsynchronousFileChannel asynchronousFileChannel = AsynchronousFileChannel.open(file.toPath(), EnumSet.of(StandardOpenOption.READ), executorService);

        ByteBuffer byteBuffer = ByteBuffer.allocate((int) asynchronousFileChannel.size());

        asynchronousFileChannel.read(byteBuffer, 0, null, new CompletionHandler<Integer, Object>() {
            @Override
            public void completed(Integer result, Object attachment) {
                if (result == -1) {
                    logger.error("파일을 읽어오는데서 문제가 발생했습니다. result =" + result);
                    return;
                }
                //파일로부터 읽어들이고 object로 변환하기
                try {
                    ConsumerGroup consumerGroup = (ConsumerGroup) avroSerializers.getDeserialization(byteBuffer.array(), consumerGroupsSchema);

                    logger.info("ConsumerGroups을 성공적으로 읽었습니다. ->" + consumerGroup);

                    groupsCompletableFuture.complete(consumerGroup);

                } catch (EOFException e) {
                    logger.info("Consumer Group 파일에 값이 존재하지 않습니다.");

                    groupsCompletableFuture.complete(new ConsumerGroup());
                    return;

                } catch (Exception e) {
                    logger.error("byte[]을 Object로 변환하는 과정에서 오류가 발생했습니다.", e);
                    groupsCompletableFuture.complete(null);
                }

                closeAsyncChannel(asynchronousFileChannel);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                logger.error("파일을 read하는데 실패했습니다.", exc);
                closeAsyncChannel(asynchronousFileChannel);
            }
        });
    }


    private void writeAsyncFileChannel(File file, Schema schema, ConsumerGroup consumerGroup) throws Exception {

        //consumer group을 정의하고 consumer가 구독할 topic들을 저장한다
        consumerGroup.setGroup_id(requestConsumerGroup.getGroup_id());

        //topic을 구독하는 consumer 리스트를 생성한다
        for (String topic : requestConsumerGroup.getTopics()) {
            List<String> consumerList = consumerGroup.getTopicMap().getOrDefault(topic, new ArrayList<>());
            consumerList.add(requestConsumerGroup.getConsumer_id());
            consumerGroup.getTopicMap().put(topic, consumerList);
        }

        CompletableFuture<Boolean> resultFuture = new CompletableFuture<>();

        groupRebalanceHandler.runRebalance(resultFuture, consumerGroup);

        //rebalance가 끝났음에 대한 결과를 리턴한다
        boolean isPossible = resultFuture.get();

        if (isPossible) {
            byte[] bytes = avroSerializers.getSerialization(consumerGroup, schema);

            ByteBuffer byteBuffer = ByteBuffer.allocate(bytes.length);
            byteBuffer.put(bytes);

            byteBuffer.flip();

            AsynchronousFileChannel asynchronousFileChannel = AsynchronousFileChannel.open(file.toPath(), EnumSet.of(StandardOpenOption.WRITE), executorService);


            asynchronousFileChannel.write(byteBuffer, 0, null, new CompletionHandler<Integer, Object>() {
                @Override
                public void completed(Integer result, Object attachment) {

                    if (result == -1) {
                        logger.error("파일을 작성하면서 문제가 발생했습니다.");
                        return;
                    }

                    //파일로 저장이 끝났음으로 consumer에게 consumerGroup을 업데이트를 알림
                    ctx.channel().writeAndFlush(new UpdateConsumerGroup());

                    logger.info("Consumer group을 성공적으로 작성하였습니다.");

                    closeAsyncChannel(asynchronousFileChannel);
                }

                @Override
                public void failed(Throwable exc, Object attachment) {
                    logger.error("Consumer group을 write하는데 실패했습니다.", exc);
                    closeAsyncChannel(asynchronousFileChannel);
                }
            });
        } else {
            logger.info("rebalance를 진행하던 중 문제가 발생했습니다.");
            ctx.channel().writeAndFlush(new ResponseError(500, "rebalance를 진행하던 중 문제가 발생했습니다."));
        }
    }


    private void closeAsyncChannel(AsynchronousFileChannel asynchronousFileChannel) {
        if (asynchronousFileChannel != null && asynchronousFileChannel.isOpen()) {
            try {
                asynchronousFileChannel.close();
            } catch (IOException e) {
                logger.error("asyncChannel을 close하는 중에 문제가 발생했습니다", e);
            }
        }
    }

}
