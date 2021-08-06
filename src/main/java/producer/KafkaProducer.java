package producer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import manager.NetworkManager;
import model.ProducerRecord;
import org.apache.log4j.Logger;


public class KafkaProducer {
    private ChannelFuture channelFuture;
    private final Logger logger = Logger.getLogger(KafkaProducer.class);
    public static final Sender sender =new Sender();

    public KafkaProducer(String host, int port) throws Exception {
        start(host, port);
    }

    public void start(String host, int port)  {
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


    public void send(ProducerRecord record) {

        if (record == null) {
            throw new NullPointerException("record가 존재하지 않습니다.");
        }
        else if (record.getTopic() == null) {
            throw new NullPointerException("topic이 존재하지 않습니다.");
        }
        else {
            sender.getTopicMetaData(channelFuture,record);
        }
    }

}
