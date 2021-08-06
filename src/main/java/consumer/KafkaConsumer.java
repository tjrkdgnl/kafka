package consumer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import manager.NetworkManager;
import org.apache.log4j.Logger;
import util.DataUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class KafkaConsumer<V> {
    private final Logger logger = Logger.getLogger(KafkaConsumer.class);
    public static Properties properties;
    private final SubscribeState subscribeState;
    private ChannelFuture channelFuture;
    private final String group_id;
    private final String session_id;

    public KafkaConsumer(Properties properties) throws Exception {
        subscribeState = new SubscribeState();

        KafkaConsumer.properties = properties;

        session_id = ("consumer-" + DataUtil.createTimestamp()).trim();
        group_id = properties.getProperty(ConsumerConfig.GROUP_ID.name());

        start();
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

        channelFuture = bootstrap.connect().sync();

    }

    public void subscribe(Collection<String> topics)  {
        if (topics == null) {
            throw new NullPointerException("topics Collections is null ");
        }

        if (topics.isEmpty()) {
            this.unSubscribe();
        } else {

            subscribeState.setSubscription(new HashSet<>(topics));
        }
    }

    private void unSubscribe() {

    }
}
