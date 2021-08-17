package brokerServer;

import io.netty.channel.ChannelHandlerContext;
import model.ConsumerGroup;
import model.request.RequestMessage;
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

    public ConsumerGroupHandler(Properties properties) {
        this.logger = Logger.getLogger(ConsumerGroupHandler.class);
        this.avroSerializers = new AvroSerializers();
        this.defaultPath = Path.of(properties.getProperty(BrokerConfig.LOG_DIRS.getValue()));

        int ioThread = Integer.parseInt(properties.getProperty(BrokerConfig.IO_THREAD.getValue()));
        executorService = Executors.newFixedThreadPool(ioThread);
        consumerGroupSchema = ReflectData.get().getSchema(ConsumerGroup.class);
        groupRebalanceHandler = new GroupRebalanceHandler();
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
                    processTheResults(file, ctx, message, byteBuffer.array());
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
        //저장된 consumerGroup을 가져온다
        consumerGroup.setGroupId(message.getGroupId());
        consumerGroup.setRebalanceId(consumerGroup.getRebalanceId() + 1);

        //topic을 구독하는 consumer 리스트를 생성한다
        if (message.getTopics() != null) {
            for (String topic : message.getTopics()) {
                List<String> consumerList = consumerGroup.getTopicMap().getOrDefault(topic, new ArrayList<>());
                if (!consumerList.contains(message.getConsumerId())) {
                    consumerList.add(message.getConsumerId());
                }
                consumerGroup.setConsumerList(topic, consumerList);
            }
        }

        groupRebalanceHandler.setListener(status -> {
            switch (status) {
                case SUCCESS:
                    //Group status 변경하기
                    writeAsyncFileChannel(file, ctx, message, consumerGroup);
                    break;
                case EXIT:
                    if (file.delete()) {
                        logger.info("ConsumerGroup을 성공적으로 삭제했습니다.");
                    } else {
                        logger.error("ConsumerGroup을 삭제하지 못했습니다.");
                    }
                    break;
                case FAIL:
                    logger.error("리밸런스를 진행하던 중 문제가 발생했습니다.");
                    break;
            }
        });

        groupRebalanceHandler.runRebalance(consumerGroup);
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

                //파일로 저장이 끝났음으로 consumer에게 consumerGroup을 업데이트를 알림
                if (message.getConsumerId() != null) {
                    UpdateGroupInfo responseGroupInfo = new UpdateGroupInfo(GroupStatus.UPDATE, message.getConsumerId());
                    ctx.channel().writeAndFlush(responseGroupInfo);

                    logger.info("Consumer group을 성공적으로 작성하였습니다.");
                }

                closeAsyncChannel(asynchronousFileChannel);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                logger.error("Consumer group을 write하는데 실패했습니다.", exc);
                closeAsyncChannel(asynchronousFileChannel);
            }
        });
    }


    private void processTheResults(File file, ChannelHandlerContext ctx, RequestMessage message, byte[] bytes) throws Exception {
        switch (message.getStatus()) {
            case REBALANCING:
                try {
                    ConsumerGroup consumerGroups = (ConsumerGroup) avroSerializers.getDeserialization(bytes, consumerGroupSchema);
                    logger.info("ConsumerGroups을 성공적으로 읽었습니다. ->" + consumerGroups);

                    executeRebalance(file, ctx, message, consumerGroups);
                } catch (EOFException e) {
                    logger.info("Consumer Group 파일에 값이 존재하지 않습니다.");

                    //처음 group에 진입하기 위한 객체 초기화 진행한다 또한 rebalance를 진행하도록 한다
                    ConsumerGroup consumerGroups = new ConsumerGroup();
                    executeRebalance(file, ctx, message, consumerGroups);

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

                //file로 관리되고 있는 consumer Group에 변화가 생겼다면 컨슈머에게 update 요청을 보낸다
                if (consumerGroup.getRebalanceId() > message.getRebalanceId()) {
                    UpdateGroupInfo responseGroupInfo = new UpdateGroupInfo(GroupStatus.UPDATE, message.getConsumerId());
                    ctx.channel().writeAndFlush(responseGroupInfo);
                } else {
                    //message 전송 하는 로직 구현
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
