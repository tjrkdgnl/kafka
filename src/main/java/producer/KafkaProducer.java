package producer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import manager.NetworkManager;
import model.ProducerRecord;
import model.request.RequestTopicMetaData;
import org.apache.log4j.Logger;

public class KafkaProducer {
    private final String host;
    private final int port;
    private ChannelFuture channelFuture;
    private final Logger logger = Logger.getLogger(KafkaProducer.class);
    public static final Sender sender = new Sender();

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


            return channelFuture;

        } catch (Exception e) {
            logger.trace(e.getStackTrace());
        }

        return null;
    }


    public void send(ProducerRecord record)  {
        channelFuture.channel().writeAndFlush(new RequestTopicMetaData(record));
    }
}
