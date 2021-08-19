package consumer;

import io.netty.channel.ChannelFuture;
import model.ConsumerGroup;
import model.request.RequestMessage;
import org.apache.log4j.Logger;
import util.MemberState;

public class Fetcher {
    private final Logger logger;
    private final ConsumerMetadata metadata;
    private final SubscribeState subscribeState;
    private final String groupId;
    private final String consumerId;
    private final ChannelFuture channelFuture;

    public Fetcher(ConsumerMetadata consumerMetadata, SubscribeState subscribeState, ChannelFuture channelFuture, String groupId, String consumerId) {
        this.logger = Logger.getLogger(Fetcher.class);
        this.metadata = consumerMetadata;
        this.subscribeState = subscribeState;
        this.channelFuture = channelFuture;
        this.groupId = groupId;
        this.consumerId = consumerId;

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

    public void pollForFetches() {
        //subscribe인 경우
        if (subscribeState.getSubscriptions().size() != 0) {
            channelFuture.channel().writeAndFlush(new RequestMessage(this.metadata.getStatus(), metadata.getRebalanceId(),
                    subscribeState.getSubscriptions(), consumerId, groupId));
        }
    }

}