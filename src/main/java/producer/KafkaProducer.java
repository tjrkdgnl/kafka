package producer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import manager.NetworkManager;
import manager.TopicMetadata;
import model.ProducerRecord;
import model.request.RequestTopicMetaData;
import org.apache.log4j.Logger;

public class KafkaProducer {
    private final String host;
    private final int port;
    private ChannelFuture channelFuture;
    private final RoundRobinPartitioner roundRobinPartitioner = new RoundRobinPartitioner();
    private final Logger logger = Logger.getLogger(KafkaProducer.class);
    private final TopicMetadata topicMetadata = TopicMetadata.newInstance();

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

            //토픽의 메타데이터 정보를 요청하여 send 전에 미리 TopicMetaData를 갖고있음
             channelFuture.channel().writeAndFlush(new RequestTopicMetaData());

             Thread.sleep(3000);

             return channelFuture;

        } catch (Exception e) {
            logger.error(e.getMessage());
        }

        return null;
    }


    public void send(Integer partition, String message) throws Exception {
        if (topicMetadata.checkPartition(partition)) {

            ProducerRecord producerRecord = new ProducerRecord(topicMetadata.getTopic(), partition, message);

            channelFuture.channel().writeAndFlush(producerRecord);

        } else {
            int partitionNumber = roundRobinPartitioner.partition(topicMetadata.getPartitions().size());

            ProducerRecord producerRecord = new ProducerRecord(topicMetadata.getTopic(), partitionNumber, message);

            channelFuture.channel().writeAndFlush(producerRecord);
        }
    }
}
