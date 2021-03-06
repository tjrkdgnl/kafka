package brokerServer;

import java.io.FileInputStream;
import java.nio.file.Path;
import java.util.Properties;

public class BrokerApplication {

    public static void main(String[] args) throws Exception {

        //추후에 입력받는 방식으로 변경하기
        Properties properties = new Properties();
        properties.put(BrokerConfig.HEARTBEAT_INTERVAL.getValue(), "6000");

        Path propertiesPath = Path.of("/Users/user/IdeaProjects/2021_SeoKangHwi/config/server.properties");

        properties.load(new FileInputStream(propertiesPath.toString()));

        BrokerServer brokerServer = new BrokerServer(properties);

        brokerServer.start();

        if (args.length != 0 && args[0].equals("--topic")) {
            brokerServer.createTopic(args[1]);
        }
    }
}

