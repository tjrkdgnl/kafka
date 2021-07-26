package producer;

import org.apache.log4j.Logger;

public class ProducerApplication {
    private static final Logger logger = Logger.getLogger(ProducerApplication.class);

    public static void main(String[] args) throws Exception {

        KafkaProducer kafkaProducer = new KafkaProducer("127.0.0.1", 8888);

        kafkaProducer.start().addListener(future -> {
            if (future.isSuccess()) {
                kafkaProducer.send(null, "this is test");

            }
            else
                System.out.println(future.cause().getMessage());
        });

    }
}

