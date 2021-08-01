package producer;

import io.netty.channel.ChannelHandlerContext;
import model.ProducerRecord;
import model.Topic;
import org.apache.log4j.Logger;
import util.ERROR;

public class Sender {
    private final Logger logger = Logger.getLogger(KafkaProducer.class);
    private final RoundRobinPartitioner roundRobinPartitioner = new RoundRobinPartitioner();

    public Sender(){

    }

    public void send(ChannelHandlerContext channelFuture, ProducerRecord record, Topic topicData) throws Exception {
        if (record.getTopic() == null) {
            logger.error("Error: topic type is " + ERROR.NO_TOPIC);
        }
        else {
            if(record.getPartition() !=null){
                if (record.getPartition() <= topicData.getPartitions() - 1) {
                    channelFuture.channel().writeAndFlush(record);
                }
                else {
                    logger.error(ERROR.INVAILD_OFFSET_NUMBER);
                }
            }
            else{
                record.setPartition(roundRobinPartitioner.partition(topicData.getPartitions()));

                channelFuture.channel().writeAndFlush(record);
            }
        }
    }

}
