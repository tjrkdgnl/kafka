package brokerServer;

import model.RecordData;
import model.schema.Records;
import model.TopicPartition;
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
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerRecordsHandler {
    private final Logger logger = Logger.getLogger(ProducerRecordHandler.class);
    private final String LOG = "log";
    private final String PARTITION = "partition";
    private final ExecutorService executorService;
    private final Schema recordsSchema;
    private final AvroSerializers avroSerializers;
    private final String brokerID;
    private final Path defaultPath;

    public ConsumerRecordsHandler(Properties properties) {
        int ioThread = Integer.parseInt(properties.getProperty(BrokerConfig.IO_THREAD.getValue()));

        executorService = Executors.newFixedThreadPool(ioThread);
        recordsSchema = ReflectData.get().getSchema(Records.class);
        avroSerializers = new AvroSerializers();
        brokerID = properties.getProperty(BrokerConfig.ID.getValue());
        defaultPath = Path.of(properties.getProperty(BrokerConfig.LOG_DIRS.getValue()));
    }

    public void readRecords(TopicPartition topicPartition, RecordListener recordListener) {
        executorService.submit(() -> {
            Path topicPath = Path.of(defaultPath + "/" + "broker-" + brokerID + "/" + topicPartition.getTopic() +
                    "/" + PARTITION + topicPartition.getPartition());
            Path logPath = Path.of(topicPath + "/" + LOG + "_");
            File logFile = new File(topicPath.toString());
            File[] logFiles = logFile.listFiles((dir, name) -> name.startsWith(LOG));

            for (int logNumber = 0; logNumber < logFiles.length; logNumber++) {
                try {
                    readProdcerRecord(logNumber, logPath, recordListener);
                } catch (IOException e) {
                    logger.error("records??? ?????? ??? ????????? ??????????????????. ", e);
                }
            }
        });
    }

    private void readProdcerRecord(int offset, Path defaultLogPath, RecordListener recordListener) throws IOException {

        //????????? ????????? record file path
        Path logPath = Path.of(defaultLogPath.toString() + offset);

        //????????? ????????? record file ????????????
        File file = new File(logPath.toString());

        //record?????? ??????
        readAsyncFileChannel(file, recordsSchema, recordListener);
    }

    private void readAsyncFileChannel(File file, Schema schema, RecordListener recordListener) throws IOException {

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
                handlingAfterReading(byteBuffer.array(), schema, recordListener);

                closeAsyncChannel(asynchronousFileChannel);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                logger.error("????????? read????????? ??????????????????.", exc);
                closeAsyncChannel(asynchronousFileChannel);
            }
        });
    }

    private void handlingAfterReading(byte[] bytes, Schema schema, RecordListener recordListener) {
        switch (schema.getName()) {
            case "Records":
                try {
                    Records records = (Records) avroSerializers.getDeserialization(bytes, schema);
                    recordListener.setRecordsData(records.getRecords());

                } catch (EOFException e) {
                    logger.info("records ????????? ?????? ???????????? ????????????.");

                } catch (Exception e) {
                    logger.error("byte[]??? Object??? ???????????? ???????????? ????????? ??????????????????.", e);
                }
                break;
        }
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

    @FunctionalInterface
    public interface RecordListener {
        void setRecordsData(List<RecordData> recordData);
    }

}

