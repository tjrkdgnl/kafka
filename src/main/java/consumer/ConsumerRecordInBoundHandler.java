package consumer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import model.ConsumerRecord;
import model.ConsumerOffsetInfo;
import model.response.ResponseConsumerRecords;
import model.response.UpdateGroupInfo;
import org.apache.log4j.Logger;
import util.DataUtil;
import util.ERROR;
import util.MemberState;

import java.util.HashMap;

public class ConsumerRecordInBoundHandler extends ChannelInboundHandlerAdapter {
    private final Logger logger = Logger.getLogger(ConsumerRecordInBoundHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            Object obj = DataUtil.parsingBufToObject((ByteBuf) msg);

            if (obj instanceof ResponseConsumerRecords) {
                ResponseConsumerRecords resConsumerRecords = (ResponseConsumerRecords) obj;
                ConsumerClient consumer = ConsumerManager.getInstance().getConsumer(resConsumerRecords.getConsumerId());

                HashMap<ConsumerOffsetInfo, Integer> unCommitedOffset = new HashMap<>();

                for (ConsumerRecord consumerRecord : resConsumerRecords.getConsumerRecords()) {
                    ConsumerOffsetInfo topicInfo = new ConsumerOffsetInfo(resConsumerRecords.getGroupId(),
                            resConsumerRecords.getConsumerId(), consumerRecord.getTopicPartition());

                    //갱신 요청할 topicInfo만 저장한다
                    unCommitedOffset.put(topicInfo, consumer.getOffset(consumerRecord.getTopicPartition()) + 1);
                }

                //커밋되지 않은 offsetinfo 저장
                consumer.setUnCommitedOffset(unCommitedOffset);
                //Application 단에서 받을 consumerRecords 저장
                ConsumerManager.getInstance().setConsumerRecords(resConsumerRecords.getConsumerId(), resConsumerRecords.getConsumerRecords());

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
            } else {
                logger.error(ERROR.UNKNOWN_ERROR);
            }

        } catch (Exception e) {
            logger.error("broker로부터 받은 msg object를 parsing하던 중 문제가 발생했습니다.", e);
        }
    }
}
