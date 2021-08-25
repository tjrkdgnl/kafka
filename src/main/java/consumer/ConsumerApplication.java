package consumer;

import model.ConsumerRecord;
import model.TopicPartition;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConsumerApplication {

    public static void main(String[] args) {
        Logger logger = Logger.getLogger(ConsumerApplication.class);
        Properties properties = new Properties();

        properties.put(ConsumerConfig.SERVER.name(), "127.0.0.1:8888");
        properties.put(ConsumerConfig.SESSION_TIMEOUT.name(), "10000");
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL.name(), "3000");
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL.name(), "5000");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS.name(), "1");
        properties.put(ConsumerConfig.GROUP_ID.name(), args[0].trim());
        properties.put(ConsumerConfig.CONSUMER_ID.name(), args[1].trim());

        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);

        try {
            if (args.length == 3) {
                kafkaConsumer.subscribe(Arrays.asList(args[2].trim()));
            } else if (args.length == 4 && StringUtils.isNumeric(args[3].trim())) {
                int partiton = Integer.parseInt(args[3]);
                kafkaConsumer.assign(Arrays.asList(new TopicPartition(args[2].trim(), partiton)));

            } else if (args.length == 5 && StringUtils.isNumeric(args[3].trim()) && StringUtils.isNumeric(args[4].trim())) {
                int partiton = Integer.parseInt(args[3]);
                kafkaConsumer.assign(Arrays.asList(new TopicPartition(args[2].trim(), partiton)));
                properties.put(ConsumerConfig.MAX_POLL_RECORDS.name(), args[4]);
            } else {
                logger.info("args를 잘못 입력 받았습니다.");
                System.exit(-1);
            }

            while (true) {
                List<ConsumerRecord> consumerRecords = kafkaConsumer.poll();

                for (ConsumerRecord consumerRecord : consumerRecords) {
                    System.out.printf("Topic: %s, Partition: %d, Offset: %d, Value: %s\n", consumerRecord.getTopicPartition().getTopic(),
                            consumerRecord.getTopicPartition().getPartition(), consumerRecord.getOffset(), consumerRecord.getMessage());
                }
            }
        } catch (Exception e) {
            logger.error("polling 중 문제가 발생했습니다.", e);
        } finally {
            kafkaConsumer.close();
        }
    }
}
