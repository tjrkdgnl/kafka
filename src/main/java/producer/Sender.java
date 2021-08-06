package producer;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import model.ProducerRecord;
import model.Topic;
import model.request.RequestTopicMetaData;
import org.apache.log4j.Logger;

public class Sender {
    private final RoundRobinPartitioner roundRobinPartitioner =new RoundRobinPartitioner();
    private final Logger logger = Logger.getLogger(Sender.class);


    public void send(ChannelHandlerContext ctx, ProducerRecord record, Topic topicMetaData) throws Exception {

        if(record.getPartition() ==null){
            record.setPartition(roundRobinPartitioner.partition(topicMetaData.getPartitions()));
        }
        else{
            if(record.getPartition() > topicMetaData.getPartitions()-1){
                logger.error("지정한 파지션의 수가 topic의 파티션 수보다 많습니다. ");
                return;
            }

        }
        ctx.channel().writeAndFlush(record);
    }

    public void getTopicMetaData(ChannelFuture channelFuture,ProducerRecord record){
        channelFuture.channel().writeAndFlush(new RequestTopicMetaData(record));
    }

}
