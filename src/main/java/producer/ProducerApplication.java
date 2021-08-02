package producer;

import model.ProducerRecord;
import org.apache.log4j.Logger;

public class ProducerApplication {
    private static final Logger logger = Logger.getLogger(ProducerApplication.class);

    public static void main(String[] args) throws Exception {

        KafkaProducer kafkaProducer = new KafkaProducer("127.0.0.1", 8888);

        kafkaProducer.start().addListener(future -> {
            if (future.isSuccess()) {
                kafkaProducer.send(new ProducerRecord("daily-report-topic", "this is test"));

            }
            else
              logger.error(future.cause().getStackTrace());
        });
    }
}

