package brokerServer;

import org.apache.log4j.Logger;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

public class BrokerProperties {
    private final Properties properties;
    private final Logger logger = Logger.getLogger(BrokerServer.class);

    public BrokerProperties(Path path) throws IOException {
        properties =new Properties();

        try {
            properties.load(new FileInputStream(path.toString()));
        }
        catch (IOException e){
            logger.error("properties를 file로부터 읽어오던 중 문제가 발생했습니다.",e);
        }

    }

    public Properties getProperties() {
        return properties;
    }
}
