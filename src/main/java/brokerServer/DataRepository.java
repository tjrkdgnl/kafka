package brokerServer;

import model.request.RequestHeartbeat;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.*;

public class DataRepository {

    private final HashMap<String, List<String>> groups;
    private final Set<String> rebalancingGroup;
    //consumer-heartbeat
    private final HashMap<String, RequestHeartbeat> consumerHeartbeat;


    public DataRepository() {
        this.groups = new HashMap<>();
        this.consumerHeartbeat = new HashMap<>();
        this.rebalancingGroup = new HashSet<>();
    }

    public static DataRepository getInstance() {
        return SingletonRepo.INSTANCE;
    }

    public List<String> getConsumers(String groupId) {
        return groups.get(groupId);
    }

    public void addRebalancingGroup(String groupId) {
        this.rebalancingGroup.add(groupId);
    }

    public HashSet<String> getRebalancingGroup() {
        return new HashSet<>(rebalancingGroup);
    }

    public void deleteRebalancingGroup(String groupId) {
        rebalancingGroup.remove(groupId);
    }

    public HashMap<String, RequestHeartbeat> getConsumerHeartbeat() {
        return new HashMap<>(consumerHeartbeat);
    }

    public void joinTheGroup(String groupId, String consumerId) {
        List<String> consumers = groups.getOrDefault(groupId, new ArrayList<>());

        if (!consumers.contains(consumerId)) {
            consumers.add(consumerId);
        }

        groups.put(groupId, consumers);
    }

    public void leaveTheGroup(String groupId, String consumerId) {
        List<String> consumers = groups.get(groupId);
        consumers.remove(consumerId);
        groups.put(groupId, consumers);
    }

    public void removeHeartbeat(String consumerId){
        consumerHeartbeat.remove(consumerId);
    }

    public void addHeartbeat(String consumerId, RequestHeartbeat requestHeartbeat) {
        consumerHeartbeat.put(consumerId, requestHeartbeat);
    }

    private static class SingletonRepo {
        private static final DataRepository INSTANCE = new DataRepository();
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
    }
}
