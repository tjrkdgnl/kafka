package producer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import manager.NetworkManager;
import manager.TopicManager;
import model.ProducerRecord;
import model.Topic;
import model.request.RequestTopicMetaData;
import org.apache.log4j.Logger;
import util.ERROR;

public class KafkaProducer {
    private final String host;
    private final int port;
    private ChannelFuture channelFuture;
    private final RoundRobinPartitioner roundRobinPartitioner = new RoundRobinPartitioner();
    private final Logger logger = Logger.getLogger(KafkaProducer.class);

    public KafkaProducer(String host, int port) throws Exception {
        this.host = host;
        this.port = port;
    }

    public ChannelFuture start() throws Exception {
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();

        try {
            Bootstrap bootstrap = NetworkManager.getInstance().createProducerClient(eventLoopGroup, host, port)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            socketChannel.pipeline().addLast(new ProducerInBoundHandler());
                            socketChannel.pipeline().addLast(new ProducerOutBoundHandler());
                        }
                    });

            channelFuture = bootstrap.connect().sync();

            //Topic List Data 얻어오기
            channelFuture.channel().writeAndFlush(new RequestTopicMetaData());

            Thread.sleep(3000);

            return channelFuture;

        } catch (Exception e) {
            logger.trace(e.getStackTrace());
        }

        return null;
    }


    public void send(String topic, String message){
        if (topic == null) {
            logger.error("Error: topic type is "+ERROR.NO_TOPIC);
            return;
        }
        else{
            if(TopicManager.getInstance().getTopicList() ==null){
                logger.error(ERROR.OBJECT_NULL+": 서버로부터 Topic Lit를 받지 못했습니다");
                return;
            }

            for(Topic topicData : TopicManager.getInstance().getTopicList()){
                if(topicData.getTopic().equals(topic)){
                    try {
                        int partitionNumber = roundRobinPartitioner.partition(topicData.getPartitions());

                        ProducerRecord producerRecord = new ProducerRecord(topic,partitionNumber, message);

                        channelFuture.channel().writeAndFlush(producerRecord);

                    } catch (Exception e) {
                       logger.trace(e.getStackTrace());
                    }
                    return;
                }
            }

            logger.info("현재 controller에 저장된 topic이 존재하지 않습니다. \n" +
                    "controller properties에서 topic 생성옵션을 변경해주세요.");

        }
    }

    public void send(String topic, int partition, String message) throws Exception {
        if (topic == null) {
            logger.error("Error: topic type is "+ERROR.NO_TOPIC);
            return;
        }

        else{
            if(TopicManager.getInstance().getTopicList() ==null){
                logger.error(ERROR.OBJECT_NULL+": 서버로부터 Topic Lit를 받지 못했습니다");
                return;
            }

            for(Topic topicData : TopicManager.getInstance().getTopicList()){
                if(topicData.getTopic().equals(topic)){
                    if(partition <=topicData.getPartitions()-1){
                        ProducerRecord producerRecord = new ProducerRecord(topic, partition, message);

                        channelFuture.channel().writeAndFlush(producerRecord);
                    }
                    else{
                        logger.error(ERROR.INVAILD_OFFSET_NUMBER);
                    }
                    return;
                }
            }

            logger.info("현재 controller에 저장된 topic이 존재하지 않습니다. \n" +
                    "controller properties에서 topic 생성옵션을 변경해주세요.");
        }
    }
}
