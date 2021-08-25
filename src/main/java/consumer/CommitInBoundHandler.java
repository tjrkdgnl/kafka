package consumer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import model.response.ResponseCommit;
import model.response.UpdateGroupInfo;
import org.apache.log4j.Logger;
import util.DataUtil;
import util.MemberState;

public class CommitInBoundHandler extends ChannelInboundHandlerAdapter {
    private final Logger logger = Logger.getLogger(CommitInBoundHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            Object obj = DataUtil.parsingBufToObject((ByteBuf) msg);

            if (obj instanceof ResponseCommit){
                ResponseCommit responseCommit = (ResponseCommit) obj;

                ConsumerClient consumer = ConsumerManager.getInstance().getConsumer(responseCommit.getConsumerId());

                if(responseCommit.getStatus() == 200){
                    consumer.updateOffset();
                }


            } else if (obj instanceof UpdateGroupInfo) {
                UpdateGroupInfo groupInfo = (UpdateGroupInfo) obj;

                ConsumerClient consumer = ConsumerManager.getInstance().getConsumer(groupInfo.getConsumerId());

                switch (groupInfo.getGroupStatus()) {
                    case UPDATE:
                        consumer.getFetcher().changeStatus(MemberState.UPDATE);
                        break;

                    case COMPLETE:
                        consumer.getFetcher().updateTopicPartitions(groupInfo.getConsumerGroup());
                        break;
                }
            }

        } catch (Exception e) {
            logger.error("broker로부터 받은 msg object를 parsing하던 중 문제가 발생했습니다.", e);
        }
    }
}
