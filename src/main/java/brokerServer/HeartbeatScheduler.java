package brokerServer;

import io.netty.channel.ChannelHandlerContext;
import model.request.RequestHeartbeat;
import model.request.RequestMessage;
import util.MemberState;

import java.util.Properties;

public class HeartbeatScheduler extends Thread {
    private final DataRepository dataRepository;
    private final ConsumerGroupHandler consumerGroupHandler;
    private final ChannelHandlerContext ctx;

    HeartbeatScheduler(Properties properties, ChannelHandlerContext ctx) {
        this.consumerGroupHandler = new ConsumerGroupHandler(properties);
        this.dataRepository = DataRepository.getInstance();
        this.ctx = ctx;
    }

    @Override
    public void run() {

        for (RequestHeartbeat heartbeat : dataRepository.getConsumerHeartbeat().values()) {
            if (System.currentTimeMillis() - heartbeat.getHeartbeatTime() > heartbeat.getSessionTimeout()) {
                dataRepository.leaveTheGroup(heartbeat.getGroupId(), heartbeat.getConsumerId());
                dataRepository.removeHeartbeat(heartbeat.getConsumerId());
                dataRepository.addRebalancingGroup(heartbeat.getGroupId());
            }
        }

        for (String groupId : dataRepository.getRebalancingGroup()) {
            RequestMessage message = new RequestMessage(MemberState.REBALANCING, groupId);
            consumerGroupHandler.checkConsumerGroup(ctx, message);
            dataRepository.deleteRebalancingGroup(groupId);
        }
    }
}
