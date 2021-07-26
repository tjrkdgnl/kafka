package producer;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import util.DataUtil;

public class ProducerOutBoundHandler extends ChannelOutboundHandlerAdapter {

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {

        if(msg != null){
            ctx.writeAndFlush(DataUtil.parsingObjectToByteBuf(msg));
        }
        else{
            throw new NullPointerException("Message Object is null");
        }
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        ctx.close();
        promise.setSuccess();
    }
}
