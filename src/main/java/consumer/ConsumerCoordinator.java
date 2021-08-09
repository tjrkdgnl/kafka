package consumer;

import io.netty.channel.ChannelFuture;
import model.request.RequestJoinGroup;
import org.apache.log4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ConsumerCoordinator {
    private final Logger logger = Logger.getLogger(ConsumerCoordinator.class);
    public static CompletableFuture<Boolean> joinGroupFuture;
    private final ConsumerMetadata metadata;
    private final SubscribeState subscribeState;


    public ConsumerCoordinator(ConsumerMetadata metadata, SubscribeState subscribeState) {
        this.metadata = metadata;
        this.subscribeState = subscribeState;
    }


    public void requestJoinGroup(CompletableFuture<Boolean> joinFuture, ChannelFuture cf, String groupId, String consumer_id) throws ExecutionException, InterruptedException {

        joinGroupFuture = new CompletableFuture<>();

        cf.channel().writeAndFlush(new RequestJoinGroup(groupId, consumer_id, this.subscribeState.getSubscriptions()));

        boolean isPossible = joinGroupFuture.get();

        if (isPossible) {
            joinFuture.complete(true);
        } else {
            logger.error("group을 join하는데 문제가 발생했습니다. ");
            joinFuture.complete(false);
        }
    }
}
