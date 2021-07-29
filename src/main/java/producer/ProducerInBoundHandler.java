package producer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import manager.TopicManager;
import model.AckData;
import model.response.ResponseTopicData;
import org.apache.log4j.Logger;
import util.DataUtil;
import util.ERROR;

import java.util.ArrayList;


public class ProducerInBoundHandler extends ChannelInboundHandlerAdapter {
    private final Logger logger =Logger.getLogger(ProducerInBoundHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        Object obj = DataUtil.parsingBufToObject((ByteBuf)msg);

       if(obj instanceof ResponseTopicData){
            logger.info("프로듀서가 브로커로부터 TopicList를 받았습니다.");
            ResponseTopicData responseTopicData = (ResponseTopicData)obj;

            if(responseTopicData.getTopicList() != null){
                TopicManager.getInstance().setTopicList(responseTopicData.getTopicList());
            }
            else{
                TopicManager.getInstance().setTopicList(new ArrayList<>());
            }
        }
        else if(obj instanceof AckData){
            AckData ack =(AckData) obj;

            if(ack.getStatus() == 200){
                logger.info(ack.getMessage());

            }
            else if(ack.getStatus() == 400){
                logger.error(ack.getMessage());
            }
            else{
                logger.error(ERROR.UNKNOWN_STATUS_ERROR +ack.getMessage());
            }
        }
        else{
            logger.error(ERROR.UNKNOWN_ERROR);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.trace(cause);
        ctx.close();
    }
}
