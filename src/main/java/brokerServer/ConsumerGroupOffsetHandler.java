package brokerServer;

import io.netty.channel.ChannelHandlerContext;
import model.schema.ConsumerGroupsOffsetInfo;
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
                    logger.info("consumer_offsets ????????? ??????????????????");
                    listener.setConsumersOffset(null);
                    return;
                }

                File offsetFile = new File(consumerOffsetPath.toString());

                readAsyncFileChannel(offsetFile, listener);


            } catch (IOException e) {
                logger.info("consumer_offsets??? ?????? ??? ????????? ??????????????????.", e);
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
                logger.info("consumer_offsets??? ?????? ??? ????????? ??????????????????.", e);
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
                logger.info("consumer_offsets??? ?????? ??? ????????? ??????????????????.", e);
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
                    logger.error("????????? ?????????????????? ????????? ??????????????????. result =" + result);
                    return;
                }
                //??????????????? ??????????????? object??? ????????????
                try {
                    ConsumerGroupsOffsetInfo consumerGroupsOffsetInfo = (ConsumerGroupsOffsetInfo) avroSerializers.getDeserialization(byteBuffer.array(), schema);

                    listener.setConsumersOffset(consumerGroupsOffsetInfo);

                } catch (EOFException e) {
                    logger.info("consumer__offset ????????? ????????? ?????? ????????????");
                    listener.setConsumersOffset(null);

                } catch (IOException e) {
                    logger.error("consumer group??? offset??? ?????? ??? ????????? ??????????????????.", e);
                }


                closeAsyncChannel(asynchronousFileChannel);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                logger.error("????????? read????????? ??????????????????.", exc);
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
                    logger.error("????????? ??????????????? ????????? ??????????????????.");
                    return;
                }

                //?????? consumerGroup??? topic offset?????? ????????????
                dataRepository.updateConsumersOffsetMap(consumerGroupsOffsetInfo.getConsumerOffsetMap());

                if (ctx != null) {
                    ctx.channel().writeAndFlush(new ResponseCommit(consumerId, 200));
                }

                closeAsyncChannel(asynchronousFileChannel);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                logger.error("Consumer group??? write????????? ??????????????????.", exc);
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
                logger.error("asyncChannel??? close?????? ?????? ????????? ??????????????????", e);
            }
        }
    }

}
