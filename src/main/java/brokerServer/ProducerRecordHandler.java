package brokerServer;

import io.netty.channel.ChannelHandlerContext;
import model.*;
import model.schema.Offsets;
import model.schema.Records;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.log4j.Logger;
import util.AvroSerializers;
import util.DataUtil;

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
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ProducerRecordHandler {
    private final Logger logger = Logger.getLogger(ProducerRecordHandler.class);
    private final String LOG = "log";
    private final String OFFSET = "offset";
    private final String PARTITION = "partition";
    private final ExecutorService executorService;
    private ChannelHandlerContext ctx;
    private final int maxSegmentSize;
    private Path defaultLogPath;
    private Path defaultOffsetPath;
    private Path topicPath;
    private final Schema offsetSchema;
    private final Schema recordsSchema;
    private final AvroSerializers avroSerializers;
    private final Properties properties;

    public ProducerRecordHandler(Properties properties) {
        int ioThread = Integer.parseInt(properties.getProperty(BrokerConfig.IO_THREAD.getValue()));

        this.properties = properties;
        executorService = Executors.newFixedThreadPool(ioThread);
        offsetSchema = ReflectData.get().getSchema(Offsets.class);
        recordsSchema = ReflectData.get().getSchema(Records.class);
        avroSerializers = new AvroSerializers();
        this.maxSegmentSize = Integer.parseInt(properties.getProperty(BrokerConfig.SEGMENT_BYTES.getValue()));
    }


    public void saveProducerRecord(ChannelHandlerContext ctx, ProducerRecord producerRecord) {
        ctx.executor().submit(() -> {
            this.ctx = ctx;

            LogOffsetListener logOffsetListener = offsets -> {
                //?????? offset list??? ???????????? ???????????? ?????? offsetData ??????
                if (offsets != null && offsets.getOffsetDataList().size() == 0) {
                    offsets.getOffsetDataList().add(new OffsetData(0, 0, 0));
                }

                RecordsListener recordsListener = records -> {
                    try {
                        writeProducerRecord(records, offsets, producerRecord);
                    } catch (IOException e) {
                        logger.error("records??? ???????????? ??? ????????? ??????????????????.", e);
                    }
                };

                try {
                    int size = offsets.getOffsetDataList().size();
                    readProdcerRecord(offsets.getOffsetDataList().get(size - 1), null, recordsListener);
                } catch (Exception e) {
                    logger.error("records??? ???????????? ??? ????????? ??????????????????.", e);
                }
            };

            try {
                String brokerID = properties.getProperty(BrokerConfig.ID.getValue());
                Path defaultPath = Path.of(properties.getProperty(BrokerConfig.LOG_DIRS.getValue()));

                topicPath = Path.of(defaultPath + "/" + "broker-" + brokerID + "/" + producerRecord.getTopic() + "/" + PARTITION + producerRecord.getPartition());
                defaultLogPath = Path.of(topicPath + "/" + LOG + "_");
                defaultOffsetPath = Path.of(topicPath + "/" + OFFSET + "_");

                if (!Files.exists(topicPath)) {
                    try {
                        Files.createDirectories(topicPath);
                        logger.info("topic directory ????????????");

                    } catch (Exception e) {
                        logger.error("topic directory??? ???????????? ???????????????", e);
                    }
                }

                readOffset(logOffsetListener);

            } catch (Exception e) {
                logger.error("offsetData??? ???????????? ??? ????????? ??????????????????.", e);
            }
        });
    }


    private void readOffset(LogOffsetListener offsetListener) throws IOException {

        File rootFile = new File(topicPath.toString());

        //????????? ???????????? ????????????
        File[] offsetFiles = rootFile.listFiles(((dir, name) -> name.startsWith(OFFSET)));

        if (offsetFiles.length == 0) {
            Path offsetPath = Files.createFile(Path.of(defaultOffsetPath.toString() + 0));

            offsetFiles = new File[1];
            offsetFiles[0] = new File(offsetPath.toString());
            logger.info("offset file ????????????");
        }

        //?????? ?????? ????????? ???????????? ??????
        DataUtil.fileSort(offsetFiles);

        //?????? ????????? ????????? offset file ????????????
        File lastOffsetFile = offsetFiles[offsetFiles.length - 1];

        //?????? offsetFile??? ??????
        readAsyncFileChannel(lastOffsetFile, offsetSchema, offsetListener, null);

    }

    private void writeOffset(Offsets offsets, OffsetData newOffsetData) throws IOException {
        int size = offsets.getOffsetDataList().size();

        //?????? ????????? ????????? offset ??????
        OffsetData lastOffsetData = offsets.getOffsetDataList().get(size - 1);

        Path offsetPath = Path.of(defaultOffsetPath.toString() + lastOffsetData.getSelfOffset());

        File lastOffsetFile = new File(offsetPath.toString());

        int lastOffsetSize = (int) lastOffsetFile.length();
        int newOffsetDataSize = newOffsetData.size();


        //record??? ???????????? ???????????? ?????? ????????? ????????? ????????????
        if (lastOffsetSize + newOffsetDataSize <= maxSegmentSize) {
            offsets.getOffsetDataList().add(newOffsetData);
            writeAsyncFileChannel(lastOffsetFile, offsetSchema, offsets, null);
        } else {
            logger.info("??? offset file??? ??????????????? ");

            //????????? offset file??? ???????????? selfOffet??? ??????????????????.
            newOffsetData.plusSelfOffset();

            //??? offset path ??????
            Path newOffsetPath = Path.of(defaultOffsetPath.toString() + newOffsetData.getSelfOffset());

            Files.createFile(newOffsetPath);

            File nextOffsetFile = new File(newOffsetPath.toString());

            //??? offset file??? ????????? offsets ?????? ?????? ??? newOffsetdata ??????
            Offsets newOffsets = new Offsets();

            newOffsets.getOffsetDataList().add(newOffsetData);

            writeAsyncFileChannel(nextOffsetFile, offsetSchema, newOffsets, null);
        }

    }


    private void readProdcerRecord(OffsetData lastOffsetData, LogOffsetListener logOffsetListener, RecordsListener recordsListener) throws IOException {

        //????????? ????????? record file path
        Path logPath = Path.of(defaultLogPath.toString() + lastOffsetData.getPhysicalOffset());
        logger.info(logPath);
        if (!Files.exists(logPath)) {
            Files.createFile(logPath);
            logger.info("LOG ?????? ?????? ??????");
        }

        //????????? ????????? record file ????????????
        File file = new File(logPath.toString());

        //record?????? ??????
        readAsyncFileChannel(file, recordsSchema, logOffsetListener, recordsListener);
    }


    private void writeProducerRecord(Records records, Offsets offsets, ProducerRecord producerRecord) throws IOException {
        int size = offsets.getOffsetDataList().size();
        OffsetData lastOffsetData = offsets.getOffsetDataList().get(size - 1);

        Path lastLogPath = Path.of(defaultLogPath.toString() + lastOffsetData.getPhysicalOffset());

        File lastLogFile = new File(lastLogPath.toString());

        //????????? newOffset ??????
        OffsetData newOffsetData = new OffsetData(lastOffsetData.getSelfOffset(), lastOffsetData.getPhysicalOffset()
                , lastOffsetData.getRelativeOffset() + 1);

        //????????? newRecord ??????
        RecordData newRecordData = new RecordData(producerRecord.getTopic(), producerRecord.getValue(),
                producerRecord.getPartition(), newOffsetData.getRelativeOffset());

        int lastLogSize = (int) lastLogFile.length();
        int newRecordSize = newRecordData.size();

        if (lastLogSize + newRecordSize <= maxSegmentSize) {
            //?????? record file??? ???????????? ???
            records.getRecords().add(newRecordData);
            writeAsyncFileChannel(lastLogFile, recordsSchema, records, producerRecord);
        } else {
            logger.info("??? ?????? ????????? ???????????????.");

            //?????? log offset??? ???????????? physical offset ??????
            newOffsetData.plusPhysicalOffset();

            Path nextLogPath = Path.of(defaultLogPath.toString() + newOffsetData.getPhysicalOffset());

            //????????? log ?????? ??????
            Files.createFile(nextLogPath);

            File nextLogFile = new File(nextLogPath.toString());

            //??? log ????????? ????????? records ?????? ???  newRecord ??????
            Records newRecords = new Records();

            newRecords.getRecords().add(newRecordData);

            writeAsyncFileChannel(nextLogFile, recordsSchema, newRecords, producerRecord);
        }

        //offset ????????????
        writeOffset(offsets, newOffsetData);
    }


    private void readAsyncFileChannel(File file, Schema schema, LogOffsetListener logOffsetListener, RecordsListener recordsListener) throws IOException {

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
                handlingAfterReading(byteBuffer.array(), schema, logOffsetListener, recordsListener);

                closeAsyncChannel(asynchronousFileChannel);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                logger.error("????????? read????????? ??????????????????.", exc);
                closeAsyncChannel(asynchronousFileChannel);
            }
        });
    }


    private void writeAsyncFileChannel(File file, Schema schema, Object value, ProducerRecord producerRecord) throws IOException {

        AsynchronousFileChannel asynchronousFileChannel = AsynchronousFileChannel.open(file.toPath(), EnumSet.of(StandardOpenOption.WRITE), executorService);

        byte[] bytes = avroSerializers.getSerialization(value, schema);

        ByteBuffer byteBuffer = ByteBuffer.allocate(bytes.length);
        byteBuffer.put(bytes);

        byteBuffer.flip();

        asynchronousFileChannel.write(byteBuffer, 0, null, new CompletionHandler<Integer, Object>() {
            @Override
            public void completed(Integer result, Object attachment) {

                if (result == -1) {
                    logger.error("????????? ??????????????? ????????? ??????????????????.");
                    return;
                }

                if (value instanceof Records) {
                    TopicPartition topicPartition = new TopicPartition(producerRecord.getTopic(), producerRecord.getPartition());
                    Records records = (Records) value;
                    DataRepository.getInstance().setRecords(topicPartition, records.getRecords());
                }

                handlingAfterWriting(schema);

                closeAsyncChannel(asynchronousFileChannel);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                logger.error("???????????? write????????? ??????????????????.", exc);
                closeAsyncChannel(asynchronousFileChannel);
            }
        });
    }


    private void handlingAfterWriting(Schema schema) {
        switch (schema.getName()) {
            case "Records":
                ctx.channel().writeAndFlush(new AckData(200, "broker??? record??? ??????????????? ?????????????????????"));
                logger.info("record??? ??????????????? ?????????????????????.");
                break;

            case "Offsets":
                logger.info("offsetData??? ??????????????? ??????????????????.");
                break;
        }
    }


    private void handlingAfterReading(byte[] bytes, Schema schema, LogOffsetListener logOffsetListener, RecordsListener recordsListener) {
        switch (schema.getName()) {
            case "Records":
                try {
                    Records records = (Records) avroSerializers.getDeserialization(bytes, schema);
                    recordsListener.setRecords(records);
                    logger.info("record??? ??????????????? ???????????????.");
                } catch (EOFException e) {
                    logger.info("records ????????? ?????? ???????????? ????????????.");

                    recordsListener.setRecords(new Records());

                    return;
                } catch (Exception e) {
                    logger.error("byte[]??? Object??? ???????????? ???????????? ????????? ??????????????????.", e);
                    recordsListener.setRecords(null);
                }

                break;

            case "Offsets":
                try {
                    Offsets offsets = (Offsets) avroSerializers.getDeserialization(bytes, schema);

                    logOffsetListener.setOffsets(offsets);
                    logger.info("??????????????? offsets??? ???????????????.");
                } catch (EOFException e) {
                    logger.info("?????? offset ????????? ?????? ???????????? ????????????");

                    logOffsetListener.setOffsets(new Offsets());
                } catch (Exception e) {
                    logger.error("byte[]??? Object??? ???????????? ???????????? ????????? ??????????????????.", e);
                    logOffsetListener.setOffsets(null);
                }

                break;
        }
    }


    @FunctionalInterface
    public interface LogOffsetListener {
        void setOffsets(Offsets offsets);
    }

    @FunctionalInterface
    public interface RecordsListener {
        void setRecords(Records records);
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
