package brokerServer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import manager.FileManager;
import model.ProducerRecord;
import model.request.RequestTopicMetaData;
import org.apache.log4j.Logger;
import util.DataUtil;

public class BrokerInBoundHandler extends ChannelInboundHandlerAdapter {

    private Logger logger = Logger.getLogger(BrokerInBoundHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        Object obj = DataUtil.parsingBufToObject((ByteBuf) msg);

        logger.info("broker server Received data from client");

        if (obj == null) {
            ctx.channel().writeAndFlush(DataUtil.parsingObjectToByteBuf("Object is null"));
        }
        else if (obj instanceof RequestTopicMetaData) {
            FileManager.getInstance().readTopicMetaData(ctx);
        }
        else if (obj instanceof ProducerRecord) {
            //message 토픽 파티션에 파일로 저장하기
            logger.info("record 저장 ");
            FileManager.getInstance().writeTopicMetaData(ctx,(ProducerRecord)obj);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }


}
