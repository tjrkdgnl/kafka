package producer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import model.AckData;
import model.Topic;
import model.response.ResponseTopicMetadata;
import org.apache.log4j.Logger;
import util.DataUtil;
import util.ERROR;

import java.util.concurrent.CompletableFuture;


public class ProducerInBoundHandler extends ChannelInboundHandlerAdapter {
    private final Logger logger = Logger.getLogger(ProducerInBoundHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        try {
            Object obj = DataUtil.parsingBufToObject((ByteBuf) msg);

            if (obj instanceof ResponseTopicMetadata) {
                logger.info("프로듀서가 브로커로부터 TopicMetaData를 받았습니다.");
                ResponseTopicMetadata responseTopicData = (ResponseTopicMetadata) obj;

                String request_id = responseTopicData.getRequest_id();

                CompletableFuture<Topic> completableFuture = KafkaProducer.topicMap.get(request_id);

                //prodcuer client에게 topic 전달
                completableFuture.complete(responseTopicData.getTopicMetadata());

            } else if (obj instanceof AckData) {
                AckData ack = (AckData) obj;

                if (ack.getStatus() == 200) {
                    logger.info(ack.getMessage());

                } else if (ack.getStatus() == 400) {
                    logger.error(ack.getMessage());
                } else {
                    logger.error(ERROR.UNKNOWN_STATUS_ERROR + ack.getMessage());
                }
            } else {
                logger.error(ERROR.UNKNOWN_ERROR);
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
