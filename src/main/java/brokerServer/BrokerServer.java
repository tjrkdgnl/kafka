package brokerServer;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import manager.NetworkManager;
import model.AckData;
import model.ProducerRecord;
import model.Topic;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import util.FileUtil;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;

public class BrokerServer {
    private final Logger logger = Logger.getLogger(BrokerServer.class);
    private final String host;
    private final int port;
    private static HashMap<String, String> properties;


    public BrokerServer(HashMap<String, String> brokerProperties) throws Exception {
        properties = brokerProperties;

        this.host = properties.get(BrokerConfig.HOST.getValue());

        if (StringUtils.isNumeric(properties.get(BrokerConfig.PORT.getValue().trim()))) {
            this.port = Integer.parseInt(properties.get(BrokerConfig.PORT.getValue()));
        } else {
            this.port = 8888;
            properties.put(BrokerConfig.PORT.getValue(), String.valueOf(port));
        }

    }

    public void start() throws Exception {

        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        EventLoopGroup workerEventLoopGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap bootstrap = NetworkManager.getInstance().buildServer(eventLoopGroup, workerEventLoopGroup, host, port);

            bootstrap.bind().sync();

        } catch (Exception e) {
            logger.trace(e.getStackTrace());
        }

    }


    public static void getTopicMetaData(ChannelHandlerContext ctx, ProducerRecord record) throws Exception {

        String defaultPath = properties.get(BrokerConfig.LOG_DIRS.getValue());

        if (defaultPath == null) {
            ctx.channel().writeAndFlush(new AckData(500, "broker log directory path가 잘못 지정되었습니다."));

        } else {
            FileUtil.getInstance(Path.of(defaultPath)).getTopicMetaData(ctx,record,properties);
        }
    }

}
