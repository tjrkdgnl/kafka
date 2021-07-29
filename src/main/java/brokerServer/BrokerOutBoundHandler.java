package brokerServer;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.apache.log4j.Logger;
import util.DataUtil;

public class BrokerOutBoundHandler extends ChannelOutboundHandlerAdapter {
    private final Logger logger =Logger.getLogger(BrokerOutBoundHandler.class);

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        logger.info("브로커가 Message를 클라이언트에게 전송했습니다.");
        ctx.writeAndFlush(DataUtil.parsingObjectToByteBuf(msg));
    }

}
