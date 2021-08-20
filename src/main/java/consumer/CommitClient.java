package consumer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import manager.NetworkManager;
import model.ConsumerOffsetInfo;
import model.request.RequestCommit;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Properties;

public class CommitClient {
    private Logger logger;
    private ChannelFuture channelFuture;

    public CommitClient(Properties properties) {
        logger = Logger.getLogger(CommitClient.class);

        try {
            String[] address = properties.getProperty(ConsumerConfig.SERVER.name()).split(":");
            String host = address[0];
            int port = Integer.parseInt(address[1].trim());
            start(host, port);


        } catch (Exception e) {
            logger.error("CommitClient가 서버에 연결하던 중 문제가 생겼습니다. ", e);
        }

    }

    private void start(String host, int port) throws Exception {
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        Bootstrap bootstrap = NetworkManager.getInstance().buildClient(eventLoopGroup, host, port)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        socketChannel.pipeline().addLast(new CommitInBoundHandler());
                        socketChannel.pipeline().addLast(new ConsumerOutBoundHandler());
                    }
                });

        channelFuture = bootstrap.connect().sync();
    }

    public void requestCommit(String consumerId,HashMap<ConsumerOffsetInfo,Integer> unCommitedMap){
        channelFuture.channel().writeAndFlush(new RequestCommit(consumerId,unCommitedMap));
    }

}
