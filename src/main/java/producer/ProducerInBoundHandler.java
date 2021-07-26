package producer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import manager.TopicMetadata;
import model.response.ResponseTopicData;
import org.apache.log4j.Logger;
import util.DataUtil;

public class ProducerInBoundHandler extends ChannelInboundHandlerAdapter {

    private final Logger logger =Logger.getLogger(ProducerInBoundHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        Object obj = DataUtil.parsingBufToObject((ByteBuf)msg);


        if(obj == null){
            logger.error("object is null");
        }
        else if(obj instanceof ResponseTopicData){
            ResponseTopicData topicData = (ResponseTopicData)obj;

            TopicMetadata.newInstance().setTopic(topicData.getTopic());
            TopicMetadata.newInstance().setPartitions(topicData.getPartitions());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.trace(cause);
        ctx.close();
    }
}
