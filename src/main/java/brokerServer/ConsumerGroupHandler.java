package brokerServer;

import io.netty.channel.ChannelHandlerContext;
import model.*;
import model.request.RequestMessage;
import model.response.ResponseConsumerRecords;
import model.response.UpdateGroupInfo;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.log4j.Logger;
import util.AvroSerializers;
import util.GroupStatus;
import util.MemberState;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerGroupHandler {
    private final Logger logger;
    private final AvroSerializers avroSerializers;
    private final Path defaultPath;
    private Path groupPath;
    private final ExecutorService executorService;
    private final Schema consumerGroupSchema;
    private final GroupRebalanceHandler groupRebalanceHandler;
    private final DataRepository dataRepository;

    public ConsumerGroupHandler(Properties properties) {
        this.logger = Logger.getLogger(ConsumerGroupHandler.class);
        this.avroSerializers = new AvroSerializers();
        this.defaultPath = Path.of(properties.getProperty(BrokerConfig.LOG_DIRS.getValue()));

        int ioThread = Integer.parseInt(properties.getProperty(BrokerConfig.IO_THREAD.getValue()));
        executorService = Executors.newFixedThreadPool(ioThread);
        consumerGroupSchema = ReflectData.get().getSchema(ConsumerGroup.class);
        groupRebalanceHandler = new GroupRebalanceHandler();
        dataRepository = DataRepository.getInstance();
    }

    public void checkConsumerGroup(ChannelHandlerContext ctx, RequestMessage message) {
        executorService.submit(() -> {
            try {
                groupPath = Path.of(defaultPath.toString() + "/" + message.getGroupId());

                if (!Files.exists(groupPath)) {
                    Files.createFile(groupPath);
                    logger.info("Consumer Group 파일 생성 완료");
                }
                File file = new File(groupPath.toString());

                readAsyncFileChannel(file, ctx, message);

            } catch (Exception e) {
                logger.error("consumerGroup file을 읽던 중 문제가 발생했습니다", e);
            }
        });
    }

    private void readAsyncFileChannel(File file, ChannelHandlerContext ctx, RequestMessage message) throws IOException {

        AsynchronousFileChannel asynchronousFileChannel = AsynchronousFileChannel.open(file.toPath(), EnumSet.of(StandardOpenOption.READ), executorService);

        ByteBuffer byteBuffer = ByteBuffer.allocate((int) asynchronousFileChannel.size());

        asynchronousFileChannel.read(byteBuffer, 0, null, new CompletionHandler<Integer, Object>() {
            @Override
            public void completed(Integer result, Object attachment) {
                if (result == -1) {
                    logger.error("파일을 읽어오는데서 문제가 발생했습니다. result =" + result);
                    return;
                }

                try {
                    processTheResult(file, ctx, message, byteBuffer.array());
                } catch (Exception e) {
                    logger.error("consumer group 처리 중 문제가 발생했습니다. ", e);
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

    private void executeRebalance(File file, ChannelHandlerContext ctx, RequestMessage message,
                                  ConsumerGroup consumerGroup) throws Exception {

        GroupRebalanceHandler.RebalanceCallbackListener listener = status -> {
            switch (status) {
                case SUCCESS:
                    writeAsyncFileChannel(file, ctx, message, consumerGroup);
                    break;
                case FAIL:
                    logger.error("리밸런스를 진행하던 중 문제가 발생했습니다.");
                    break;
            }
        };

        if (message.getStatus() == MemberState.JOIN)
            groupRebalanceHandler.runRebalance(consumerGroup, message, listener);
        else if (message.getStatus() == MemberState.REMOVE) {
            groupRebalanceHandler.runRebalanceForRemoving(consumerGroup, listener);
        }
    }

    private void writeAsyncFileChannel(File file, ChannelHandlerContext ctx, RequestMessage message, ConsumerGroup consumerGroup) throws Exception {

        byte[] bytes = avroSerializers.getSerialization(consumerGroup, consumerGroupSchema);

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

                if (ctx != null) {
                    ctx.channel().writeAndFlush(new UpdateGroupInfo(GroupStatus.UPDATE, message.getConsumerId()));
                }
                logger.info("Consumer group을 성공적으로 작성하였습니다.");

                closeAsyncChannel(asynchronousFileChannel);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                logger.error("Consumer group을 write하는데 실패했습니다.", exc);
                closeAsyncChannel(asynchronousFileChannel);
            }
        });
    }


    private void processTheResult(File file, ChannelHandlerContext ctx, RequestMessage message, byte[] bytes) throws Exception {
        switch (message.getStatus()) {
            case JOIN:
                try {
                    ConsumerGroup consumerGroup = (ConsumerGroup) avroSerializers.getDeserialization(bytes, consumerGroupSchema);
                    logger.info("ConsumerGroups을 성공적으로 읽었습니다. ->" + consumerGroup);

                    if (consumerGroup.checkConsumer(message.getConsumerId())) {
                        ctx.channel().writeAndFlush(new UpdateGroupInfo(GroupStatus.UPDATE, message.getConsumerId()));
                    } else {
                        executeRebalance(file, ctx, message, consumerGroup);
                    }

                } catch (EOFException e) {
                    logger.info("Consumer Group 파일에 값이 존재하지 않습니다.");

                    //처음 group에 진입하기 위한 객체 초기화 진행한다 또한 rebalance를 진행하도록 한다
                    ConsumerGroup consumerGroups = new ConsumerGroup();
                    executeRebalance(file, ctx, message, consumerGroups);

                } catch (Exception e) {
                    logger.error("byte[]을 Object로 변환하는 과정에서 오류가 발생했습니다.", e);
                }
                break;

            case REMOVE:
                try {
                    ConsumerGroup consumerGroup = (ConsumerGroup) avroSerializers.getDeserialization(bytes, consumerGroupSchema);
                    executeRebalance(file, null, message, consumerGroup);
                } catch (Exception e) {
                    logger.error("byte[]을 Object로 변환하는 과정에서 오류가 발생했습니다.", e);
                }
                break;

            case UPDATE:
                try {
                    ConsumerGroup consumerGroup = (ConsumerGroup) avroSerializers.getDeserialization(bytes, consumerGroupSchema);
                    ctx.channel().writeAndFlush(new UpdateGroupInfo(GroupStatus.COMPLETE, consumerGroup, message.getConsumerId()));
                } catch (IOException e) {
                    logger.error("업데이트를 진행하던 중 문제가 발생했습니다. ", e);
                }
                break;

            case STABLE:
                ConsumerGroup consumerGroup = (ConsumerGroup) avroSerializers.getDeserialization(bytes, consumerGroupSchema);

                if (consumerGroup.getRebalanceId() > message.getRebalanceId()) {
                    //file로 관리되고 있는 consumer Group에 변화가 생겼다면 컨슈머에게 update 요청을 보낸다
                    ctx.channel().writeAndFlush(new UpdateGroupInfo(GroupStatus.UPDATE, message.getConsumerId()));
                } else {
                    //컨슈머와 맵핑된 TopicPartitions
                    List<TopicPartition> topicPartitions = consumerGroup.getOwnershipMap().get(message.getConsumerId());

                    //컨슈머에게 전송할 consumerRecords
                    List<ConsumerRecord> consumerRecords = new ArrayList<>();


                    for (TopicPartition topicPartition : topicPartitions) {
                        //offset metadata를 불러오기 위해서 topicInfo key를 생성하여 offset value를 얻어온다
                        ConsumerOffsetInfo consumerOffsetInfo = new ConsumerOffsetInfo(message.getGroupId(), message.getConsumerId(), topicPartition);

                        //현재 partition에 존재하는 records를 불러온다
                        List<RecordData> records = DataRepository.getInstance().getRecords(topicPartition);
                        int offset = dataRepository.getConsumerOffsetMap().getOrDefault(consumerOffsetInfo, 1);
                        int maxRecordSize = message.getRecordSize() + offset;

                        for (RecordData recordData : records) {
                            if (offset == maxRecordSize) break;

                            ConsumerRecord consumerRecord = new ConsumerRecord(topicPartition, recordData.getOffset(), recordData.getMessage());

                            if (offset == recordData.getOffset() && !consumerRecords.contains(consumerRecord)) {
                                consumerRecords.add(consumerRecord);
                                offset++;
                            }
                        }
                    }
                    ctx.channel().writeAndFlush(new ResponseConsumerRecords(message.getGroupId(), message.getConsumerId(), consumerRecords));
                }
                break;
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
