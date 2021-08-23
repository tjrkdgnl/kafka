package consumer;

import model.ConsumerRecord;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConsumerApplication {

    public static void main(String[] args) {

        Properties properties = new Properties();

        properties.put(ConsumerConfig.SERVER.name(), "127.0.0.1:8888");
        properties.put(ConsumerConfig.GROUP_ID.name(), "test_group");
        properties.put(ConsumerConfig.CONSUMER_ID.name(), "Consumer-0");
        properties.put(ConsumerConfig.SESSION_TIMEOUT.name(), "10000");
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL.name(), "4000");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS.name(), "1");


        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);

        kafkaConsumer.subscribe(Arrays.asList("test"));

        try {
            while (true) {
                List<ConsumerRecord> consumerRecords = kafkaConsumer.poll(4000);

                for (ConsumerRecord consumerRecord : consumerRecords) {
                    System.out.printf("Topic: %s, Partition: %d, Offset: %d, Value: %s\n", consumerRecord.getTopicPartition().getTopic(),
                            consumerRecord.getTopicPartition().getPartition(), consumerRecord.getOffset(), consumerRecord.getMessage());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaConsumer.close();
        }
    }
}
