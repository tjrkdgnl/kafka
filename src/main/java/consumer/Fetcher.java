package consumer;

import io.netty.channel.ChannelFuture;
import model.ConsumerGroup;
import model.request.RequestPollingMessage;
import org.apache.log4j.Logger;
import util.ConsumerRequestStatus;

public class Fetcher {
    private final Logger logger;
    private final ConsumerMetadata metadata;
    private final SubscribeState subscribeState;
    private ConsumerRequestStatus status;
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

        status = ConsumerRequestStatus.JOIN;
    }

    public void changeStatus(ConsumerRequestStatus status) {
        this.status = status;
    }

    public void updateConsumerGroup(ConsumerGroup consumerGroup) {
        this.metadata.setRebalanceId(consumerGroup.getRebalanceId());

        this.status = ConsumerRequestStatus.MESSAGE;

        this.metadata.setTopicPartitions(consumerGroup.getOwnershipMap().get(consumerId));

        logger.info("consumerGroup-> "+ consumerGroup);
        logger.info("업데이트 완료");
    }


    public void pollForFetches() {
        channelFuture.channel().writeAndFlush(new RequestPollingMessage(status, metadata.getRebalanceId(),
                subscribeState.getSubscriptions(), consumerId, groupId));
    }

}
