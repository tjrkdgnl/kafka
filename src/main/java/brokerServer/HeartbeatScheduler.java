package brokerServer;

import model.request.RequestHeartbeat;
import model.request.RequestMessage;
import util.MemberState;

import java.util.Properties;

public class HeartbeatScheduler extends Thread {
    private final DataRepository dataRepository;
    private final ConsumerGroupHandler consumerGroupHandler;

    HeartbeatScheduler(Properties properties) {
        this.dataRepository = DataRepository.getInstance();
        this.consumerGroupHandler = new ConsumerGroupHandler(properties);
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
            RequestMessage message = new RequestMessage(MemberState.REMOVE, groupId);
            consumerGroupHandler.checkConsumerGroup(null, message);
            dataRepository.deleteRebalancingGroup(groupId);
        }
    }
}
