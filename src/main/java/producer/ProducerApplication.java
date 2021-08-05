package producer;

import model.ProducerRecord;

public class ProducerApplication {

    public static void main(String[] args) throws Exception {

        KafkaProducer kafkaProducer = new KafkaProducer("127.0.0.1", 8888);

        kafkaProducer.send(new ProducerRecord("test", "this is test"));
    }
}

