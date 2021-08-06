package consumer;

import io.netty.channel.ChannelFuture;
import model.request.RequestJoinConsumerGroup;
import org.apache.log4j.Logger;

public class ConsumerCoordinator {
    private final Logger logger = Logger.getLogger(ConsumerCoordinator.class);
    private SubscribeState subscribeState;

    public ConsumerCoordinator(SubscribeState subscribeState){
        this.subscribeState = subscribeState;
    }


    public void initJoinGroup(ChannelFuture channelFuture, String consumerGroupName,String member_id){
        channelFuture.channel().writeAndFlush(new RequestJoinConsumerGroup(consumerGroupName,member_id));
    }

}
