package brokerServer;

import model.Record;
import model.Records;
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
import java.util.ArrayList;
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

    public void readRecords(String topic, RecordListener recordListener) {
        executorService.submit(() -> {
            List<Record> records = new ArrayList<>();

            Path topicPath = Path.of(defaultPath + "/" + "broker-" + brokerID + "/" + topic);
            File topicFile = new File(topicPath.toString());

            for (int number = 0; number < topicFile.listFiles().length; number++) {
                Path partitionPath = Path.of(topicPath + "/" + PARTITION + number);
                Path logPath = Path.of(partitionPath + "/" + LOG + "_");

                File logFile = new File(partitionPath.toString());

                File[] logFiles = logFile.listFiles((dir, name) -> name.startsWith(LOG));

                for (int logNumber = 0; logNumber < logFiles.length; logNumber++) {
                    try {
                        readProdcerRecord(logNumber, logPath, records);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            recordListener.setRecordsData(records);
        });
    }

    private void readProdcerRecord(int offset, Path defaultLogPath, List<Record> records) throws IOException {

        //최근에 작성한 record file path
        Path logPath = Path.of(defaultLogPath.toString() + offset);

        //최근에 작성한 record file 불러오기
        File file = new File(logPath.toString());

        //record파일 읽기
        readAsyncFileChannel(file, recordsSchema, records);
    }

    private void readAsyncFileChannel(File file, Schema schema, List<Record> records) throws IOException {

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
                handlingAfterReading(byteBuffer.array(), schema, records);

                closeAsyncChannel(asynchronousFileChannel);
            }

            @Override
            public void failed(Throwable exc, Object attachment) {
                logger.error("파일을 read하는데 실패했습니다.", exc);
                closeAsyncChannel(asynchronousFileChannel);
            }
        });
    }

    private void handlingAfterReading(byte[] bytes, Schema schema, List<Record> recordList) {
        switch (schema.getName()) {
            case "Records":
                try {
                    Records records = (Records) avroSerializers.getDeserialization(bytes, schema);

                    for (Record record : records.getRecords()) {
                        if (!recordList.contains(record)) {
                            recordList.add(record);
                        }
                    }

                } catch (EOFException e) {
                    logger.info("records 파일에 값이 존재하지 않습니다.");

                } catch (Exception e) {
                    logger.error("byte[]을 Object로 변환하는 과정에서 오류가 발생했습니다.", e);
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

    @FunctionalInterface
    public interface RecordListener {
        void setRecordsData(List<Record> records);
    }

}
