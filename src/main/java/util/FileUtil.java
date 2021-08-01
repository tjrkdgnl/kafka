package util;


import brokerServer.BrokerConfig;
import brokerServer.BrokerServer;
import io.netty.channel.ChannelHandlerContext;
import model.AckData;
import model.ProducerRecord;
import model.Topic;
import model.response.ResponseTopicData;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class FileUtil {
    private static Path defaultPath;
    private static final String TOPIC_LIST = "topicList";
    private final Logger logger = Logger.getLogger(FileUtil.class);


    private FileUtil() {
    }

    public static FileUtil getInstance(Path path) {
        defaultPath = path;

        return SingletonFileUtil.fileUtil;
    }

    public void getTopicMetaData(ChannelHandlerContext ctx, ProducerRecord record, HashMap<String, String> properties) throws Exception {
        ctx.executor().execute(()-> {

                try {
                    Path path = Path.of(defaultPath + "/" + TOPIC_LIST + ".avsc");

                    if (!Files.exists(path)) {

                        Files.createFile(path);
                        logger.info("topicList file 생성 완료했습니다. 현재 topicList는 0개입니다.");
                    }

                    File file = new File(path.toString());

                    ArrayList<Topic> topics = new ArrayList<>();

                    DatumReader<Topic> reader = new ReflectDatumReader<>(Topic.class);
                    DataFileReader<Topic> dataFileReader = new DataFileReader<Topic>(file, reader);


                    while (dataFileReader.hasNext()) {
                        topics.add(dataFileReader.next());
                    }

                    dataFileReader.close();

                    logger.info("topicMetaData를 가져오는데 성공했습니다 ");


                    //요청한 토픽이 존재하는지 체크
                    for (Topic topicData : topics) {
                        if (topicData.getTopic().equals(record.getTopic())) {
                            ctx.channel().writeAndFlush(new ResponseTopicData(topicData, record));

                            return;
                        }
                    }

                    String autoCreateTopic = properties.get(BrokerConfig.AUTO_CREATE_TOPIC.getValue());
                    int partitions = parseStringToInteger(properties.get(BrokerConfig.TOPIC_PARTITIONS.getValue()));
                    int replicationFactor = parseStringToInteger(properties.get(BrokerConfig.REPLICATION_FACTOR.getValue()));

                    //만약 요청한 토픽이 topicList에 존재하지 않으면 topic을 생성하고 client에게 전송한다
                    createTopic(ctx, topics, record,partitions,replicationFactor, autoCreateTopic );


                } catch (IOException e) {
                    logger.trace(e.getStackTrace());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
    }

    public void createTopic(ChannelHandlerContext ctx, List<Topic> topics,
                            ProducerRecord record,int partitions,int replicationFactor,String autoCreateTopic) throws Exception {

        ctx.executor().execute(() -> {
            try {
                if (autoCreateTopic.equals("false")) {
                    ctx.channel().writeAndFlush(new AckData(500, "\"" +record.getTopic() + "\""  + "은 broker에 존재하지 않습니다."));
                    return;
                }

                //todo controller에 연결된 브로커 개수에 따라서 replication-factor가 정해질 수 있어야 한다
//                if ( replicationFactor > 현재 controller와 연결된 broker들의 개수) {
//                    logger.error("현재 controller에 연결된 broker들의 수보다 replication-factor 값이 더 큽니다");
//                    return;
//                }

                //todo partition의 leader가 될 broker들을 선정하는 로직이 필요하다. 마찬가지로 controller 구현 후 작업해야 할 것 같다.
//              int brokerLeader =  elect brokerLeader using Round robin fashion

                Path path = Path.of(defaultPath + "/" + TOPIC_LIST + ".avsc");

                if (!Files.exists(path)) {
                    Files.createFile(path);
                    logger.info(TOPIC_LIST + " file 생성 완료");
                }

                File file = new File(path.toString());

                //class type의 schema 생성
                Schema schema = ReflectData.get().getSchema(Topic.class);

                DatumWriter<Topic> datumWriter = new ReflectDatumWriter<>(Topic.class);
                DataFileWriter<Topic> dataFileWriter = new DataFileWriter<>(datumWriter);

                //지정된 스키마 형태로 파일을 작성할 수 있는 writer 생성
                dataFileWriter.create(schema, file);

                //현재는 싱글 브로커라고 가정했기 때문에 임의의 값으로 토픽을 생성한다
                Topic newTopic = new Topic(record.getTopic(), 3, "0", 0);
                topics.add(newTopic);

                //topicList에 새로운 topic을 추가하고 topicMetadata file에 저장한다
                for (Topic topicData : topics) {
                    dataFileWriter.append(topicData);
                }

                dataFileWriter.close();


                ctx.channel().writeAndFlush(new ResponseTopicData(newTopic,record));
                logger.info("topic 생성 완료 | file size: " + file.length() + " bytes");

            } catch (IOException e) {
                logger.trace(e.getStackTrace());
            }
        });
    }

    private  int parseStringToInteger(String stringNumber) {
        if (StringUtils.isNumeric(stringNumber)) {
            return Integer.parseInt(stringNumber);
        }
        return -1;
    }

    private static class SingletonFileUtil {
        private static final FileUtil fileUtil = new FileUtil();
    }
}
