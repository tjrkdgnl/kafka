package producer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import model.AckData;
import model.response.ResponseTopicMetadata;
import org.apache.log4j.Logger;
import util.DataUtil;


public class ProducerInBoundHandler extends ChannelInboundHandlerAdapter {
    private final Logger logger = Logger.getLogger(ProducerInBoundHandler.class);
    private final Sender sender = new Sender();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        try {
            Object obj = DataUtil.parsingBufToObject((ByteBuf) msg);

            if (obj instanceof ResponseTopicMetadata) {
                ResponseTopicMetadata responseTopicData = (ResponseTopicMetadata) obj;

                sender.send(ctx, responseTopicData.getProducerRecord(), responseTopicData.getTopicMetadata());

            } else if (obj instanceof AckData) {
                AckData ack = (AckData) obj;

                if (ack.getStatus() == 200) {
                    logger.info(ack.getMessage());

                } else if (ack.getStatus() == 400) {
                    logger.error(ack.getMessage());
                } else {
                    logger.error(ack.getMessage());
                }
            }
        } catch (Exception e) {
            logger.error("broker로부터 받은 msg object를 parsing하던 중 문제가 발생했습니다.", e);
        }

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("InboundHandler error", cause);
        ctx.close();
    }
}
