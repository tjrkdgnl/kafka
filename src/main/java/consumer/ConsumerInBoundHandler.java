package consumer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import model.request.UpdateConsumerGroup;
import model.response.ResponseError;
import model.response.ResponseGroupInfo;
import org.apache.log4j.Logger;
import util.DataUtil;
import util.ERROR;

public class ConsumerInBoundHandler extends ChannelInboundHandlerAdapter {
    private final Logger logger =Logger.getLogger(ConsumerInBoundHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            Object obj = DataUtil.parsingBufToObject((ByteBuf) msg);


            if(obj instanceof ResponseGroupInfo){
                ResponseGroupInfo consumerGroupInfo = (ResponseGroupInfo)obj;

                logger.info("consumer group-> "+consumerGroupInfo);

                Fetcher.groupFuture.complete(consumerGroupInfo.getConsumerGroup());


            } else if(obj instanceof UpdateConsumerGroup){
                //Broker에게서 consumer Group을 업데이트하라는 메세지를 받는다

                //consumer group에 join 완료
                ConsumerCoordinator.joinGroupFuture.complete(true);

            }
            else if(obj instanceof ResponseError){
                ResponseError error = (ResponseError)obj;

                if(error.getStatus() ==500){
                    logger.info(error.getMessage());
                }
                Fetcher.groupFuture.complete(null);
                ConsumerCoordinator.joinGroupFuture.complete(null);
            }
            else{
                logger.info(ERROR.UNKNOWN_STATUS_ERROR);
                Fetcher.groupFuture.complete(null);
                ConsumerCoordinator.joinGroupFuture.complete(null);
            }

        } catch (Exception e){
            logger.error("broker로부터 받은 msg object를 parsing하던 중 문제가 발생했습니다.", e);
            Fetcher.groupFuture.complete(null);
            ConsumerCoordinator.joinGroupFuture.complete(null);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ctx.close();
        logger.error("channel error ",cause);
    }
}
