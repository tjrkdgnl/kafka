package brokerServer;

import io.netty.channel.ChannelHandlerContext;
import model.ConsumerGroupsOffsetInfo;
import model.ConsumerOffsetInfo;
import model.request.RequestCommit;
import model.response.ResponseCommit;
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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerGroupOffsetHandler {
    private final Logger logger;
    private final ExecutorService executorService;
    private final AvroSerializers avroSerializers;
    private final Schema schema;
    private final DataRepository dataRepository;
    private final Path consumerOffsetPath;

    public ConsumerGroupOffsetHandler(Properties properties) {
        logger = Logger.getLogger(ConsumerGroupOffsetHandler.class);

        int ioThread = Integer.parseInt(properties.getProperty(BrokerConfig.IO_THREAD.getValue()));
        executorService = Executors.newFixedThreadPool(ioThread);
        avroSerializers = new AvroSerializers();
        Path defaultPath = Path.of(properties.getProperty(BrokerConfig.LOG_DIRS.getValue()));
        consumerOffsetPath = Path.of(defaultPath + "/" + "consumer_offset");
        schema = ReflectData.get().getSchema(ConsumerGroupsOffsetInfo.class);
        dataRepository = DataRepository.getInstance();
    }

    public void readConsumersOffset(ConsumersOffsetListener listener) {
        executorService.submit(() -> {
            try {
                if (!Files.exists(consumerOffsetPath)) {
                    Files.createFile(consumerOffsetPath);
                    logger.info("consumer_offsets 파일을 생성했습니다");
                    listener.setConsumersOffset(null);
                    return;
                }

                File offsetFile = new File(consumerOffsetPath.toString());

                readAsyncFileChannel(offsetFile, listener);


            } catch (IOException e) {
                logger.info("consumer_offsets을 읽던 중 문제가 발생했습니다.", e);
            }
        });
    }

    public void updateConsumerOffset() {
        executorService.submit(() -> {
            try {
                File file = new File(consumerOffsetPath.toString());

                ConsumerGroupsOffsetInfo consumerGroupsOffsetInfo =
                        new ConsumerGroupsOffsetInfo(DataRepository.getInstance().getConsumerOffsetMap());

                writeAsyncFileChannel(file, null, consumerGroupsOffsetInfo, null);
            } catch (Exception e) {
                logger.info("consumer_offsets을 쓰던 중 문제가 발생했습니다.", e);
            }
        });
    }

    public void changeConsumerOffset(RequestCommit commit, ChannelHandlerContext ctx) {
        executorService.submit(() -> {
            try {
                File file = new File(consumerOffsetPath.toString());

                HashMap<ConsumerOffsetInfo, Integer> currentOffsetInfo = dataRepository.getConsumerOffsetMap();

                currentOffsetInfo.putAll(commit.getOffsetInfo());

                ConsumerGroupsOffsetInfo consumerGroupsOffsetInfo = new ConsumerGroupsOffsetInfo(currentOffsetInfo);

                writeAsyncFileChannel(file, ctx, consumerGroupsOffsetInfo, commit.getConsumerId());
            } catch (Exception e) {
                logger.info("consumer_offsets을 쓰던 중 문제가 발생했습니다.", e);
            }
        });
    }


    private void readAsyncFileChannel(File file, ConsumersOffsetListener listener) throws IOException {

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
                    ConsumerGroupsOffsetInfo consumerGroupsOffsetInfo = (ConsumerGroupsOffsetInfo) avroSerializers.getDeserialization(byteBuffer.array(), schema);

                    listener.setConsumersOffset(consumerGroupsOffsetInfo);

                } catch (EOFException e) {
                    logger.info("consumer__offset 파일에 작성된 것이 없습니다");
                    listener.setConsumersOffset(null);

                } catch (IOException e) {
                    logger.error("consumer group의 offset을 읽던 중 문제가 발생했습니다.", e);
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


    private void writeAsyncFileChannel(File file, ChannelHandlerContext ctx, ConsumerGroupsOffsetInfo consumerGroupsOffsetInfo, String consumerId) throws Exception {

        byte[] bytes = avroSerializers.getSerialization(consumerGroupsOffsetInfo, schema);

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

                //최신 consumerGroup의 topic offset으로 업데이트
                dataRepository.updateConsumersOffsetMap(consumerGroupsOffsetInfo.getConsumerOffsetMap());

                if (ctx != null) {
                    ctx.channel().writeAndFlush(new ResponseCommit(consumerId, 200));
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


    @FunctionalInterface
    public interface ConsumersOffsetListener {
        void setConsumersOffset(ConsumerGroupsOffsetInfo consumersOffset);
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
