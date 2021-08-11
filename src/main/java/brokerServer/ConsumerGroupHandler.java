package brokerServer;

import io.netty.channel.ChannelHandlerContext;
import model.ConsumerGroup;
import model.ConsumerGroups;
import model.request.RequestPollingMessage;
import model.response.ResponseError;
import model.response.UpdateGroupInfo;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.log4j.Logger;
import util.AvroSerializers;
import util.GroupStatus;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerMessageHandler {
    private final Logger logger;
    private final AvroSerializers avroSerializers;
    private ChannelHandlerContext ctx;
    private final Path defaultPath;
    private Path groupPath;
    private RequestPollingMessage consumerMessage;
    private final ExecutorService executorService;
    private final Schema consumerGroupsSchema;
    private final HashMap<String, CompletableFuture<ConsumerGroups>> groupMap;
    private final GroupRebalanceHandler groupRebalanceHandler;
    private final ExecutorService sequentialExecutor;
    private final HashMap<String, GroupStatus> groupStatusHashMap;

    public ConsumerMessageHandler(Properties properties) {
        this.logger = Logger.getLogger(ConsumerMessageHandler.class);
        this.avroSerializers = new AvroSerializers();
        this.defaultPath = Path.of(properties.getProperty(BrokerConfig.LOG_DIRS.getValue()));

        int ioThread = Integer.parseInt(properties.getProperty(BrokerConfig.IO_THREAD.getValue()));
        executorService = Executors.newFixedThreadPool(ioThread);
        sequentialExecutor = Executors.newSingleThreadExecutor();
        consumerGroupsSchema = ReflectData.get().getSchema(ConsumerGroups.class);
        groupRebalanceHandler = new GroupRebalanceHandler();

        groupMap = new HashMap<>();
        groupStatusHashMap =new HashMap<>();
    }


    public ConsumerMessageHandler init(ChannelHandlerContext ctx, RequestPollingMessage message) {
        this.ctx = ctx;
        this.consumerMessage = message;
        return this;
    }

    public void joinConsumerGroup(){
        groupStatusHashMap.put(consumerMessage.getGroupId(),GroupStatus.JOIN);
    }


    public void checkMessage() {
        sequentialExecutor.submit(() -> {
            try {
                groupPath = Path.of(defaultPath.toString() + "/" + consumerMessage.getGroupId());
                logger.info(groupPath);

                if (!Files.exists(groupPath)) {
                    Files.createFile(groupPath);
                    logger.info("Consumer Group 파일 생성 완료");
                }

                CompletableFuture<ConsumerGroups> groupFuture = new CompletableFuture<>();
                groupMap.put(consumerMessage.getConsumerId(), groupFuture);

                readGroupMetadata();

            } catch (Exception e) {
                logger.error("consumerGroup file을 읽던 중 문제가 발생했습니다", e);
                ctx.channel().writeAndFlush(new ResponseError(500, "consumer group file을 읽던 중 문제가 발생했습니다"));
            }
        });

        sequentialExecutor.submit(() -> {
            try {
                CompletableFuture<ConsumerGroups> future = groupMap.get(consumerMessage.getConsumerId());
                ConsumerGroups consumerGroup = future.get();

                if(consumerGroup != null){
                    writeGroupMetadata(consumerGroup);
                }

            } catch (Exception e) {
                logger.error("consumerGroup file을 작성하던 중 문제가 발생했습니다", e);
                ctx.channel().writeAndFlush(new ResponseError(500, "consumer group file을 작성하던 중 문제가 발생했습니다"));
            }
        });
    }

    public void readGroupMetadata() throws Exception {
        //최근에 작성한 consumer group file 불러오기
        File file = new File(groupPath.toString());

        //ConsumerGroup 파일 읽기
        readAsyncFileChannel(file);
    }

    public void writeGroupMetadata(ConsumerGroups consumerGroups) throws Exception {
        //최근에 작성한 consumer file 불러오기
        File file = new File(groupPath.toString());

        writeAsyncFileChannel(file, consumerGroupsSchema, consumerGroups);
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

                CompletableFuture<ConsumerGroups> groupsCompletableFuture = groupMap.get(consumerMessage.getConsumerId());

                try {
                    ConsumerGroups consumerGroups = (ConsumerGroups) avroSerializers.getDeserialization(byteBuffer.array(), consumerGroupsSchema);

                    ConsumerGroup consumerGroup = consumerGroups.getGroupInfoMap().get(consumerMessage.getGroupId());

                    logger.info("ConsumerGroup을 성공적으로 읽었습니다. ->" + consumerGroup);

                    if (!consumerGroup.getOwnershipMap().containsKey(consumerMessage.getConsumerId())) {
                        //컨슈머와 ownership 관계가 있는 토픽이 없다면 rebalance 진행 후 컨슈머에게 consumer 정보 업데이트 요청
                        groupsCompletableFuture.complete(consumerGroups);
                    } else if (consumerGroup.getRebalanceId() > consumerMessage.getRebalanceId()) {
                        //컨슈머에게 group 정보 업데이트 요청 전송.
                        ctx.channel().writeAndFlush(new UpdateGroupInfo(consumerGroup));
                        groupsCompletableFuture.complete(null);

                    } else {
                        //컨슈머가 구독하고 있는 topic에 대한 message를 전송하도록 구현
                        groupsCompletableFuture.complete(null);
                    }

                } catch (EOFException e) {
                    logger.info("Consumer Group 파일에 값이 존재하지 않습니다.");

                    //처음 group에 진입하기 위한 객체 초기화 진행한다 또한 rebalance를 진행하도록 한다
                    groupsCompletableFuture.complete(new ConsumerGroups());
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


    private void writeAsyncFileChannel(File file, Schema schema, ConsumerGroups consumerGroups) throws Exception {

        //저장된 consumerGroup을 가져온다
        ConsumerGroup consumerGroup = consumerGroups.getGroupInfoMap().getOrDefault(consumerMessage.getGroupId(), new ConsumerGroup());
        consumerGroup.setGroupId(consumerMessage.getGroupId());
        consumerGroup.setRebalanceId(consumerGroup.getRebalanceId() + 1);

        //topic을 구독하는 consumer 리스트를 생성한다
        for (String topic : consumerMessage.getTopics()) {
            List<String> consumerList = consumerGroup.getTopicMap().getOrDefault(topic, new ArrayList<>());
            consumerList.add(consumerMessage.getConsumerId());
            consumerGroup.setConsumerList(topic, consumerList);
        }

        CompletableFuture<Boolean> resultFuture = new CompletableFuture<>();

        groupRebalanceHandler.runRebalance(resultFuture, consumerGroup);

        //rebalance가 끝났음에 대한 결과를 리턴한다
        boolean isPossible = resultFuture.get();

        if (isPossible) {

            consumerGroups.getGroupInfoMap().put(consumerMessage.getGroupId(), consumerGroup);

            byte[] bytes = avroSerializers.getSerialization(consumerGroups, schema);

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
                    ctx.channel().writeAndFlush(new UpdateGroupInfo(consumerGroup));

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
