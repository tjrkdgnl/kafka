package brokerServer;

import java.nio.file.Path;
import java.util.HashMap;

public class BrokerApplication {

    public static void main(String[] args) throws Exception {

        //추후에 입력받는 방식으로 변경하기
        BrokerProperties brokerProperties = new BrokerProperties(
                Path.of("/Users/user/IdeaProjects/2021_SeoKangHwi/config/server.properties"));


        BrokerServer brokerServer = new BrokerServer(brokerProperties.getProperties());

        brokerServer.start();

    }
}

