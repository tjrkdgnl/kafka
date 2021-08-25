package producer;

import model.ProducerRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.util.Scanner;

public class ProducerApplication {

    public static void main(String[] args) throws Exception {
        Logger logger = Logger.getLogger(ProducerApplication.class);
        KafkaProducer kafkaProducer = new KafkaProducer("127.0.0.1", 8888);

        Scanner scanner = new Scanner(System.in);

        logger.info("args[0]: topic, args[1]: partition, args[2]: value");


        while (true) {
            try {
                String[] arr = scanner.nextLine().split(",");

                if (StringUtils.isNumeric(arr[1])) {
                    int partition = Integer.parseInt(arr[1].trim());
                    kafkaProducer.send(new ProducerRecord(arr[0].trim(), partition, arr[2].trim()));
                } else {
                    kafkaProducer.send(new ProducerRecord(arr[0].trim(), arr[1].trim()));
                }

            } catch (Exception e) {
                logger.error("args를 parsing하는데 문제가 발생했습니다.", e);
                System.exit(-1);
            }
        }
    }
}

