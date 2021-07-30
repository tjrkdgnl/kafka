package brokerServer;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import manager.NetworkManager;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;

public class BrokerServer {
    private final String host;
    private final int port;
    public static HashMap<String, String> properties;
    private ChannelFuture channelFuture;

    public BrokerServer(HashMap<String,String> brokerProperties) throws Exception {
        properties = brokerProperties;

        this.host = properties.get(BrokerConfig.HOST.getValue());

        if(StringUtils.isNumeric(properties.get(BrokerConfig.PORT.getValue().trim()))){
            this.port = Integer.parseInt(properties.get(BrokerConfig.PORT.getValue()));
        }
        else{
            this.port = 8888;
            properties.put(BrokerConfig.PORT.getValue(), String.valueOf(port));
        }

    }

    public void start() throws Exception {

        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        EventLoopGroup workerEventLoopGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap bootstrap = NetworkManager.getInstance().buildServer(eventLoopGroup, workerEventLoopGroup, host, port);

            channelFuture = bootstrap.bind().sync();

            channelFuture.channel().closeFuture().sync();

        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            eventLoopGroup.shutdownGracefully().sync();
        }
    }

}
