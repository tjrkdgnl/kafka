package consumer;

import io.netty.channel.ChannelFuture;
import model.request.RequestJoinGroup;
import model.response.ResponseGroupInfo;
import org.apache.log4j.Logger;

import java.util.concurrent.CompletableFuture;

public class ConsumerCoordinator {
    private final Logger logger = Logger.getLogger(ConsumerCoordinator.class);
    public static CompletableFuture<ResponseGroupInfo> groupCompletableFuture;
    private final ConsumerMetadata metadata;
    private final SubscribeState subscribeState;


    public ConsumerCoordinator(ConsumerMetadata metadata, SubscribeState subscribeState) {
        this.metadata = metadata;
        this.subscribeState = subscribeState;
    }


    public void requestJoinGroup(CompletableFuture<Boolean> resultFuture,ChannelFuture cf, String groupId, String consumer_id) {

        try {
            groupCompletableFuture = new CompletableFuture<>();

            cf.channel().writeAndFlush(new RequestJoinGroup(groupId, consumer_id, this.subscribeState.getSubscriptions()));

            ResponseGroupInfo responseGroupInfo = groupCompletableFuture.get();

            this.metadata.setGroupInfo(responseGroupInfo.getConsumerGroup());

        } catch (Exception e) {
            logger.error("Join Request 요청을 보내던 중 문제가 발생했습니다.", e);
            resultFuture.complete(false);
        }

        resultFuture.complete(true);
    }
}
