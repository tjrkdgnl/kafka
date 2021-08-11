package consumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerApplication {

    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();

        properties.put(ConsumerConfig.SERVER.name(), "127.0.0.1:8888");
        properties.put(ConsumerConfig.GROUP_ID.name(), "test_group");
        properties.put(ConsumerConfig.CONSUMER_ID.name(), "Consumer-0");


        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);

        kafkaConsumer.subscribe(Arrays.asList("test"));

        kafkaConsumer.poll();

    }
}
