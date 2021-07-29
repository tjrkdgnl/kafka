package producer;

import org.apache.log4j.Logger;

public class ProducerApplication {
    private static final Logger logger = Logger.getLogger(ProducerApplication.class);

    public static void main(String[] args) throws Exception {

        KafkaProducer kafkaProducer = new KafkaProducer("127.0.0.1", 8888);

        kafkaProducer.start().addListener(future -> {
            if (future.isSuccess()) {
                kafkaProducer.send("daily-report-topic",1, "this is test");

            }
            else
              logger.trace(future.cause().getStackTrace());
        });
    }
}

