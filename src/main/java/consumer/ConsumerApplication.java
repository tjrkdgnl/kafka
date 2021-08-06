package consumer;

import java.util.Properties;

public class ConsumerApplication {

    public static void main(String[] args) throws Exception {

        Properties properties =new Properties();

        properties.put(ConsumerConfig.SERVER.name(),"127.0.0.1:8888");
        properties.put(ConsumerConfig.GROUP_ID.name(),"test_group");

        KafkaConsumer<String> kafkaConsumer =new KafkaConsumer<>(properties);

    }

}
