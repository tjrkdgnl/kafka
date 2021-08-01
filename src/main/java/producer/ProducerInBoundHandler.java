package producer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import model.AckData;
import model.response.ResponseTopicData;
import org.apache.log4j.Logger;
import util.DataUtil;
import util.ERROR;


public class ProducerInBoundHandler extends ChannelInboundHandlerAdapter {
    private final Logger logger =Logger.getLogger(ProducerInBoundHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        try {
            Object obj = DataUtil.parsingBufToObject((ByteBuf)msg);

            if(obj instanceof ResponseTopicData){
                logger.info("프로듀서가 브로커로부터 TopicData를 받았습니다.");
                ResponseTopicData responseTopicData = (ResponseTopicData)obj;
                logger.info(responseTopicData.getTopicData().getTopic());
                //클라이언트가 생성한 record의 정보를 broker로부터 얻어온 후, 전송한다
                KafkaProducer.sender.send(ctx,responseTopicData.getRecord(),responseTopicData.getTopicData());

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
        catch (Exception e){
            logger.trace(e.getStackTrace());
        }

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.trace(cause);
        ctx.close();
    }
}
