package brokerServer;

import io.netty.channel.ChannelHandlerContext;
import model.*;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ProducerRecordHandler {
    private final Logger logger = Logger.getLogger(ProducerRecordHandler.class);
    private final String LOG = "log";
    private final String OFFSET = "offset";
    private final String PARTITION = "partition";
    private final ExecutorService executorService;
    private final CompletableFuture<Object> offsetFuture;
    private final CompletableFuture<Object> recordsFuture;
    private ProducerRecord producerRecord;
    private ChannelHandlerContext ctx;
    private Offsets offsets;
    private int maxSegmentSize;
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
        offsetFuture = new CompletableFuture<>();
        recordsFuture = new CompletableFuture<>();
        offsetSchema = ReflectData.get().getSchema(Offsets.class);
        recordsSchema = ReflectData.get().getSchema(Records.class);
        avroSerializers = new AvroSerializers();
    }

    public ProducerRecordHandler init(ChannelHandlerContext ctx, ProducerRecord producerRecord) {
        this.ctx = ctx;
        this.maxSegmentSize = Integer.parseInt(properties.getProperty(BrokerConfig.SEGMENT_BYTES.getValue()));
        this.producerRecord = producerRecord;

        String brokerID = properties.getProperty(BrokerConfig.ID.getValue());
        Path defaultPath = Path.of(properties.getProperty(BrokerConfig.LOG_DIRS.getValue()));

        topicPath = Path.of(defaultPath + "/" + "broker-" + brokerID + "/" + producerRecord.getTopic() + "/" + PARTITION + producerRecord.getPartition());
        defaultLogPath = Path.of(topicPath + "/" + LOG + "_");
        defaultOffsetPath = Path.of(topicPath + "/" + OFFSET + "_");

        return this;
    }

    public void saveProducerRecord() {
        //chain을 통해서 비동기적으로 파일을 offset과 records를 읽고 쓰는 작업을 실행
        CompletableFuture.supplyAsync(() -> {
            try {
                if (!Files.exists(topicPath)) {
                    try {
                        Files.createDirectories(topicPath);
                        logger.info("topic directory 생성완료");

                    } catch (Exception e) {
                        logger.error("topic directory를 생성하지 못했습니다", e);
                    }
                }

                readOffset();

                offsets = (Offsets) offsetFuture.get();

                //만약 offset list가 존재하지 않는다면 초기 offsetData 셋팅
                if (offsets.getOffsetDataList().size() == 0) {
                    offsets.getOffsetDataList().add(new OffsetData(0, 0, 0));
                }
                int size = offsets.getOffsetDataList().size();

                //최신 offset 파일의 offsets을 전달
                return offsets.getOffsetDataList().get(size - 1);

            } catch (Exception e) {
                logger.error("offsetData를 가져오는 중 문제가 발생했습니다.", e);
            }

            return null;

        }).thenApplyAsync(offsetData -> {
            try {
                //offsets의 마지막 offsetData를 전달
                readProdcerRecord(offsetData);

                return (Records) recordsFuture.get();

            } catch (Exception e) {
                logger.error("records를 가져오는 중 문제가 발생했습니다.", e);
            }

            return null;

        }).thenAcceptAsync((records) -> {
            try {
                int size = offsets.getOffsetDataList().size();

                writeProducerRecord(records, offsets.getOffsetDataList().get(size - 1));

            } catch (Exception e) {
                logger.error("records를 작성하던 중 문제가 발생했습니다.", e);
            }
        });
    }


    private void readOffset() throws IOException {

        File rootFile = new File(topicPath.toString());

        //오프셋 파일들만 읽어온다
        File[] offsetFiles = rootFile.listFiles(((dir, name) -> name.startsWith(OFFSET)));

        if (offsetFiles.length == 0) {
            Path offsetPath = Files.createFile(Path.of(defaultOffsetPath.toString() + 0));

            offsetFiles = new File[1];
            offsetFiles[0] = new File(offsetPath.toString());
            logger.info("offset file 생성완료");
        }

        //가장 최신 파일을 찾기위해 정렬
        DataUtil.fileSort(offsetFiles);

        //가장 최근에 작성된 offset file 가져오기
        File lastOffsetFile = offsetFiles[offsetFiles.length - 1];

        //최근 offsetFile을 읽기
        readAsyncFileChannel(lastOffsetFile, offsetSchema);

    }

    private void writeOffset(Offsets offsets, OffsetData newOffsetData) throws IOException {
        int size = offsets.getOffsetDataList().size();

        //가장 최근에 생성된 offset 확인
        OffsetData lastOffsetData = offsets.getOffsetDataList().get(size - 1);

        Path offsetPath = Path.of(defaultOffsetPath.toString() + lastOffsetData.getSelfOffset());

        File lastOffsetFile = new File(offsetPath.toString());

        int lastOffsetSize = (int) lastOffsetFile.length();
        int newOffsetDataSize = newOffsetData.size();


        //record의 사이즈가 최대치를 넘지 않으면 그대로 저장한다
        if (lastOffsetSize + newOffsetDataSize <= maxSegmentSize) {
            offsets.getOffsetDataList().add(newOffsetData);
            writeAsyncFileChannel(lastOffsetFile, offsetSchema, offsets);
        } else {
            logger.info("새 offset file을 생성합니다 ");

            //새로운 offset file을 구분짓는 selfOffet을 증가시켜준다.
            newOffsetData.plusSelfOffset();

            //새 offset path 생성
            Path newOffsetPath = Path.of(defaultOffsetPath.toString() + newOffsetData.getSelfOffset());

            Files.createFile(newOffsetPath);

            File nextOffsetFile = new File(newOffsetPath.toString());

            //새 offset file에 저장할 offsets 객체 생성 및 newOffsetdata 생성
            Offsets newOffsets = new Offsets();

            newOffsets.getOffsetDataList().add(newOffsetData);

            writeAsyncFileChannel(nextOffsetFile, offsetSchema, newOffsets);
        }

    }


    private void readProdcerRecord(OffsetData lastOffsetData) throws IOException {

        //최근에 작성한 record file path
        Path logPath = Path.of(defaultLogPath.toString() + lastOffsetData.getPhysicalOffset());
        logger.info(logPath);
        if (!Files.exists(logPath)) {
            Files.createFile(logPath);
            logger.info("LOG 파일 생성 완료");
        }

        //최근에 작성한 record file 불러오기
        File file = new File(logPath.toString());

        //record파일 읽기
        readAsyncFileChannel(file, recordsSchema);
    }


    private void writeProducerRecord(Records records, OffsetData lastOffsetData) throws IOException {

        Path lastLogPath = Path.of(defaultLogPath.toString() + lastOffsetData.getPhysicalOffset());

        File lastLogFile = new File(lastLogPath.toString());

        //추가될 newOffset 작성
        OffsetData newOffsetData = new OffsetData(lastOffsetData.getSelfOffset(), lastOffsetData.getPhysicalOffset()
                , lastOffsetData.getRelativeOffset() + 1);

        //추가될 newRecord 작성
        Record newRecord = new Record(producerRecord.getTopic(), producerRecord.getValue(),
                producerRecord.getPartition(), newOffsetData.getRelativeOffset());

        int lastLogSize = (int) lastLogFile.length();
        int newRecordSize = newRecord.size();

        if (lastLogSize + newRecordSize <= maxSegmentSize) {
            //현재 record file이 여유로울 때
            records.getRecords().add(newRecord);
            writeAsyncFileChannel(lastLogFile, recordsSchema, records);
        } else {
            logger.info("새 로그 파일을 생성합니다.");

            //최신 log offset을 가르키는 physical offset 증가
            newOffsetData.plusPhysicalOffset();

            Path nextLogPath = Path.of(defaultLogPath.toString() + newOffsetData.getPhysicalOffset());

            //새로운 log 파일 생성
            Files.createFile(nextLogPath);

            File nextLogFile = new File(nextLogPath.toString());

            //새 log 파일에 작성할 records 생성 후  newRecord 저장
            Records newRecords = new Records();

            newRecords.getRecords().add(newRecord);

            writeAsyncFileChannel(nextLogFile, recordsSchema, newRecords);
        }

        //offset 업데이트
        writeOffset(offsets, newOffsetData);
    }


    private void readAsyncFileChannel(File file, Schema schema) throws IOException {

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
                handlingAfterReading(byteBuffer.array(), schema);

                closeAsyncChannel(asynchronousFileChannel);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                logger.error("파일을 read하는데 실패했습니다.", exc);
                closeAsyncChannel(asynchronousFileChannel);
            }
        });
    }


    private void writeAsyncFileChannel(File file, Schema schema, Object value) throws IOException {

        AsynchronousFileChannel asynchronousFileChannel = AsynchronousFileChannel.open(file.toPath(), EnumSet.of(StandardOpenOption.WRITE), executorService);

        byte[] bytes = avroSerializers.getSerialization(value, schema);

        ByteBuffer byteBuffer = ByteBuffer.allocate(bytes.length);
        byteBuffer.put(bytes);

        byteBuffer.flip();

        asynchronousFileChannel.write(byteBuffer, 0, null, new CompletionHandler<Integer, Object>() {
            @Override
            public void completed(Integer result, Object attachment) {

                if (result == -1) {
                    logger.error("파일을 작성하면서 문제가 발생했습니다.");
                    return;
                }

                handlingAfterWriting(schema);

                closeAsyncChannel(asynchronousFileChannel);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                logger.error("레코드를 write하는데 실패했습니다.", exc);
                closeAsyncChannel(asynchronousFileChannel);
            }
        });
    }


    private void handlingAfterWriting(Schema schema) {
        switch (schema.getName()) {
            case "Records":
                ctx.channel().writeAndFlush(new AckData(200, "broker가 record를 성공적으로 작성하였습니다"));
                logger.info("record를 성공적으로 작성하였습니다.");
                break;

            case "Offsets":
                logger.info("offsetData를 성공적으로 작성했습니다.");
                break;
        }
    }


    private void handlingAfterReading(byte[] bytes, Schema schema) {
        switch (schema.getName()) {
            case "Records":
                try {
                    Records records = (Records) avroSerializers.getDeserialization(bytes, schema);
                    recordsFuture.complete(records);
                    logger.info("record를 성공적으로 읽었습니다.");
                } catch (EOFException e) {
                    logger.info("records 파일에 값이 존재하지 않습니다.");

                    recordsFuture.complete(new Records());

                    return;
                } catch (Exception e) {
                    logger.error("byte[]을 Object로 변환하는 과정에서 오류가 발생했습니다.", e);
                    recordsFuture.complete(null);
                }

                break;

            case "Offsets":
                try {
                    Offsets offsets = (Offsets) avroSerializers.getDeserialization(bytes, schema);

                    offsetFuture.complete(offsets);
                    logger.info("성공적으로 offsets을 읽었습니다.");
                } catch (EOFException e) {
                    logger.info("현재 offset 파일은 값이 존재하지 않습니다");

                    offsetFuture.complete(new Offsets());
                } catch (Exception e) {
                    logger.error("byte[]을 Object로 변환하는 과정에서 오류가 발생했습니다.", e);
                    offsetFuture.complete(null);
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
