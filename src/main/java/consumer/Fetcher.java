package consumer;

import io.netty.channel.ChannelFuture;
import model.ConsumerGroup;
import model.ConsumerOffsetInfo;
import model.request.RequestMessage;
import org.apache.log4j.Logger;
import util.MemberState;

import java.util.HashMap;
import java.util.Properties;

public class Fetcher {
    private final Logger logger;
    private final ConsumerMetadata metadata;
    private final SubscribeState subscribeState;
    private final String groupId;
    private final String consumerId;
    private final ChannelFuture channelFuture;
    private final ConsumerRecordClient consumerRecordClient;
    private final CommitClient commitClient;

    public Fetcher(Properties properties, ConsumerMetadata consumerMetadata, SubscribeState subscribeState, ChannelFuture channelFuture, String groupId, String consumerId) {
        this.logger = Logger.getLogger(Fetcher.class);
        this.metadata = consumerMetadata;
        this.subscribeState = subscribeState;
        this.channelFuture = channelFuture;
        this.groupId = groupId;
        this.consumerId = consumerId;
        this.consumerRecordClient = new ConsumerRecordClient(properties);
        this.commitClient = new CommitClient(properties);
    }

    public void changeStatus(MemberState status) {
        this.metadata.setStatus(status);
    }

    public void updateTopicPartitions(ConsumerGroup consumerGroup) {
        this.metadata.setRebalanceId(consumerGroup.getRebalanceId());

        this.metadata.setTopicPartitions(consumerGroup.getOwnershipMap().get(consumerId));

        logger.info("consumerGroup-> " + consumerGroup);
        logger.info("업데이트 완료");

        this.metadata.setRebalanceId(consumerGroup.getRebalanceId());
        this.metadata.setTopicPartitions(consumerGroup.getOwnershipMap().get(consumerId));
        this.metadata.setStatus(MemberState.STABLE);
    }

    public void setUnCommitedOffsetInfo(HashMap<ConsumerOffsetInfo, Integer> unCommitedOffset) {
        this.metadata.setUncommittedOffset(unCommitedOffset);
    }

    public void pollForFetches() {
        if (metadata.getStatus() == MemberState.STABLE) {
            if (this.metadata.checkUnCommitedOffset()) {
                //커밋처리가 됐을 때 다음 consumerRecord를 얻기위해 request를 보낸다
                consumerRecordClient.requestConsumerRecords(this.metadata.getStatus(), metadata.getRebalanceId(),
                        subscribeState.getSubscriptions(), consumerId, groupId);
            } else {
                //커밋처리가 안되어 있는 경우 커밋처리 요청을 보낸다
                commitClient.requestCommit(consumerId, this.metadata.getUncommittedOffset());
            }
        } else {
            channelFuture.channel().writeAndFlush(new RequestMessage(this.metadata.getStatus(), metadata.getRebalanceId(),
                    subscribeState.getSubscriptions(), consumerId, groupId));
        }
    }
}
