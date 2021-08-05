package producer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import manager.NetworkManager;
import model.ProducerRecord;
import model.Topic;
import model.request.RequestTopicMetaData;
import org.apache.log4j.Logger;
import util.DataUtil;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class KafkaProducer {
    private ChannelFuture channelFuture;
    private final Logger logger = Logger.getLogger(KafkaProducer.class);
    public static HashMap<String, CompletableFuture<Topic>> topicMap = new HashMap<>();
    private final RoundRobinPartitioner roundRobinPartitioner = new RoundRobinPartitioner();

    public KafkaProducer(String host, int port) throws Exception {
        start(host, port);
    }

    public void start(String host, int port) throws Exception {
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

        } catch (Exception e) {
            logger.error("클라이언트를 실행하던 중 문제가 발생했습니다.", e);
        }
    }


    public void send(ProducerRecord record) throws Exception {

        if (record == null) {
            throw new NullPointerException("record가 존재하지 않습니다.");
        }
        else if (record.getTopic() == null) {
            throw new NullPointerException("topic이 존재하지 않습니다.");
        }
        else {
            String producer_id = "producer-" + record.getTopic() + "-" + DataUtil.createTimestamp();

            topicMap.computeIfAbsent(producer_id, future -> new CompletableFuture<>());

            //토픽 정보 요청
            channelFuture.channel().writeAndFlush(new RequestTopicMetaData(producer_id, record.getTopic()));

            Future<Topic> future = topicMap.get(producer_id);

            //inBoundHandler로부터 topicMetaData를 얻는다
            Topic topic = future.get();

            if (record.getPartition() == null) {
                //partition이 지정되어 있지 않다면 파티셔로부터 임의의 파티션을 할당받는다
                record.setPartition(roundRobinPartitioner.partition(topic.getPartitions()));

            } else if (record.getPartition() > topic.getPartitions() - 1) {
                //topic의 파티션 수보다 많은 partition 번호를 입력하면 에러가 발생하도록 한다
                logger.error("지정한 partition 번호는 topic의 partition 수보다 많습니다.");
                return;
            }

            channelFuture.channel().writeAndFlush(record);
        }
    }

}
