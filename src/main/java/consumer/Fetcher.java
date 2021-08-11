package consumer;

import io.netty.channel.ChannelFuture;
import model.ConsumerGroup;
import model.request.RequestPollingMessage;
import org.apache.log4j.Logger;
import util.ConsumerRequestStatus;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

public class Fetcher {
    private final Logger logger;
    private final ConsumerMetadata metadata;
    private static HashMap<String, CompletableFuture<ConsumerGroup>> responseMap;
    private final SubscribeState subscribeState;
    private ConsumerRequestStatus status;
    private final String groupId;
    private final String consumerId;
    private ChannelFuture channelFuture;

    public Fetcher(ConsumerMetadata consumerMetadata, SubscribeState subscribeState, String groupId, String consumerId) {
        this.logger = Logger.getLogger(Fetcher.class);
        this.metadata = consumerMetadata;
        this.subscribeState = subscribeState;
        responseMap = new HashMap<>();

        this.groupId = groupId;
        this.consumerId = consumerId;

        status = ConsumerRequestStatus.JOIN;
    }

    public void setChannelFuture(ChannelFuture channelFuture) {
        this.channelFuture = channelFuture;
    }

    public void joinConsumerGroup() {
        try {
            CompletableFuture<ConsumerGroup> groupFuture = new CompletableFuture<>();
            responseMap.put(groupId, groupFuture);

            channelFuture.channel().writeAndFlush(new RequestPollingMessage(status, metadata.getRebalanceId(),
                    subscribeState.getSubscriptions(), consumerId, groupId));

            ConsumerGroup consumerGroup = groupFuture.get();

            this.metadata.setRebalanceId(consumerGroup.getRebalanceId());

            this.status = ConsumerRequestStatus.MESSAGE;

            this.metadata.setTopicPartitions(consumerGroup.getOwnershipMap().get(consumerId));

            logger.info("consumerGroup-> "+ consumerGroup);
            logger.info("업데이트 완료");

        } catch (Exception e) {
            logger.error("consumerGroup에 join하던 중 문제가 발생했습니다.", e);
        }
    }


    public void pollForFetches() {
        channelFuture.channel().writeAndFlush(new RequestPollingMessage(status, metadata.getRebalanceId(),
                subscribeState.getSubscriptions(), consumerId, groupId));
    }

    public static HashMap<String, CompletableFuture<ConsumerGroup>> getResponseMap() {
        return responseMap;
    }

    public ConsumerRequestStatus getStatus() {
        return status;
    }
}
