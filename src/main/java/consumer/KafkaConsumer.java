package consumer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import manager.NetworkManager;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Properties;

public class KafkaConsumer {
    private Logger logger = Logger.getLogger(KafkaConsumer.class);
    private Properties properties;

    public KafkaConsumer(Properties properties) {

        try {
            this.properties = properties;
            start();
        } catch (Exception e) {
           logger.info("server와 연결 중 문제가 발생했습니다.",e);
           System.exit(-1);
        }
    }

    public void start() throws Exception {
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();

        String[] address = properties.getProperty(ConsumerConfig.SERVER.name()).split(":");

        String host = address[0];
        int port = Integer.parseInt(address[1].trim());

        logger.info("host: " + host + " port: " + port);

        Bootstrap bootstrap = NetworkManager.getInstance().createProducerClient(eventLoopGroup, host, port)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        socketChannel.pipeline().addLast(new ConsumerInBoundHandler());
                        socketChannel.pipeline().addLast(new ConsumerOutBoundHandler());
                    }
                });

        ChannelFuture channelFuture = bootstrap.connect().sync();

        ConsumerClient.getInstance().init(properties, channelFuture);

    }

    public void subscribe(Collection<String> topics){
        ConsumerClient.getInstance().subscribe(topics);
    }


    public void poll() throws InterruptedException {
        //join부터 update까지 테스트를 위한 sleep
        Thread.sleep(2000);
        ConsumerClient.getInstance().poll();
    }

}
