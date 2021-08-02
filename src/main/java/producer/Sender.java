package producer;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import model.ProducerRecord;
import model.Topic;
import model.request.RequestTopicMetaData;
import org.apache.log4j.Logger;
import util.ERROR;

public class Sender {
    private final Logger logger = Logger.getLogger(KafkaProducer.class);
    private final RoundRobinPartitioner roundRobinPartitioner = new RoundRobinPartitioner();
    private ProducerRecord record;

    public Sender(){

    }

    public void send(ChannelHandlerContext channelFuture, Topic topicMetadata) throws Exception {
        if (record.getTopic() == null) {
            logger.error("Error: topic type is " + ERROR.NO_TOPIC);
        }
        else {
            if(record.getPartition() !=null){
                if (record.getPartition() <= topicMetadata.getPartitions() - 1) {
                    channelFuture.channel().writeAndFlush(record);
                }
                else {
                    logger.error(ERROR.INVAILD_OFFSET_NUMBER);
                }
            }
            else{
                record.setPartition(roundRobinPartitioner.partition(topicMetadata.getPartitions()));

                channelFuture.channel().writeAndFlush(record);
            }
        }
    }

    public void getTopicMetaData(ChannelFuture cf, ProducerRecord producerRecord){
        this.record =producerRecord;

        cf.channel().writeAndFlush(new RequestTopicMetaData(record));
    }

}
