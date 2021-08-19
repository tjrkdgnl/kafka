package consumer;

import io.netty.channel.ChannelFuture;
import model.request.RequestHeartbeat;

public class Heartbeat extends Thread {
    private final ChannelFuture channelFuture;
    private final String groupId;
    private final String consumerId;
    private final int sessionTimeout;

    public Heartbeat(ChannelFuture channelFuture, String groupId, String consumerId, int sessionTimeout) {
        this.channelFuture = channelFuture;
        this.groupId = groupId;
        this.consumerId = consumerId;
        this.sessionTimeout = sessionTimeout;
    }

    @Override
    public void run() {
        channelFuture.channel().writeAndFlush(new RequestHeartbeat(groupId, consumerId, sessionTimeout, System.currentTimeMillis()));
    }
}
