package brokerServer;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;

public class BrokerProperties {
    private HashMap<String, String> properties;

    public BrokerProperties(Path path) throws IOException {
        initProperty();

        File file = new File(path.toString());

        List<String> propertyList = FileUtils.readLines(file, "UTF-8");

        String[] property = null;

        for (String line : propertyList) {
            property = line.split("=");

            if (property.length == 2) {
                properties.put(property[0], property[1]);
            }
        }
    }


    public void initProperty() {
        properties = new HashMap<>();

        for (BrokerConfig config : BrokerConfig.values()) {
            switch (config) {
                case ID:
                    properties.put(BrokerConfig.ID.getValue(), "0");
                    break;

                case HOST:
                    properties.put(BrokerConfig.HOST.getValue(),"127.0.0.1");
                    break;

                case PORT:
                    properties.put(BrokerConfig.HOST.getValue(), "8888");
                    break;

                case SEGMENT_BYTES:
                    properties.put(BrokerConfig.SEGMENT_BYTES.getValue(),"1073741824");
                    break;

                case TOPIC_PARTITIONS:
                    properties.put(BrokerConfig.TOPIC_PARTITIONS.getValue(),"3");
                    break;

                case AUTO_CREATE_TOPIC:
                    properties.put(BrokerConfig.AUTO_CREATE_TOPIC.getValue(),"true");
                    break;

                case REPLICATION_FACTOR:
                    properties.put(BrokerConfig.REPLICATION_FACTOR.getValue(),"3");

            }
        }
    }

    public HashMap<String, String> getProperties() {
        return properties;
    }
}
