package brokerServer;

import model.request.RequestHeartbeat;


public class HeartbeatHandler {
    private final DataRepository dataRepository;

    public HeartbeatHandler() {
        dataRepository = DataRepository.getInstance();
    }

    public void checkHeartbeat(RequestHeartbeat heartbeat) {
        dataRepository.addHeartbeat(heartbeat.getConsumerId(), heartbeat);
        dataRepository.joinTheGroup(heartbeat.getGroupId(), heartbeat.getConsumerId());
    }
}
