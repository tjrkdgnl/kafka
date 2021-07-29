package manager;


import brokerServer.BrokerInBoundHandler;
import brokerServer.BrokerOutBoundHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;

/***
 * client-server connect를 위한 server 및 client 객체를 생성해주는 class
 *
 */
public class NetworkManager {

    private NetworkManager(){

    }

    public static NetworkManager getInstance(){
        return SingleTonNetworkManager.networkManager;
    }

    public Bootstrap createProducerClient(EventLoopGroup eventLoopGroup, String host, int port) throws Exception {
        Bootstrap bootstrap = new Bootstrap();

        bootstrap.group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .remoteAddress(new InetSocketAddress(host, port));

        return bootstrap;
    }

    public ServerBootstrap buildServer(EventLoopGroup eventLoopGroup,EventLoopGroup workerEventLoopGroup,String host,int port) throws Exception{
        ServerBootstrap bootstrap = new ServerBootstrap();

        bootstrap.group(eventLoopGroup,workerEventLoopGroup)
                .channel(NioServerSocketChannel.class)
                .localAddress(new InetSocketAddress(host,port))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        socketChannel.pipeline().addLast(new BrokerInBoundHandler());
                        socketChannel.pipeline().addLast(new BrokerOutBoundHandler());
                    }
                });

        return bootstrap;
    }

    private static class SingleTonNetworkManager {
        public static final NetworkManager networkManager = new NetworkManager();
    }

}
