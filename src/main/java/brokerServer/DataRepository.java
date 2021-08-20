package brokerServer;

import model.ConsumerOffsetInfo;
import model.Record;
import model.request.RequestHeartbeat;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.*;

public class DataRepository {

    private final HashMap<String, List<String>> groups;
    private final Set<String> rebalancingGroup;
    //consumer-heartbeat
    private final HashMap<String, RequestHeartbeat> consumerHeartbeat;
    //ConsumerTopicInfo-offset
    private final HashMap<ConsumerOffsetInfo, Integer> consumersOffsetMap;
    //topic-records
    private final HashMap<String, List<Record>> recordsMap;

    public DataRepository() {
        this.groups = new HashMap<>();
        this.consumerHeartbeat = new HashMap<>();
        this.rebalancingGroup = new HashSet<>();
        this.consumersOffsetMap = new HashMap<>();
        this.recordsMap = new HashMap<>();
    }

    public static DataRepository getInstance() {
        return SingletonRepo.INSTANCE;
    }

    public HashMap<ConsumerOffsetInfo, Integer> getConsumerOffsetMap() {
        return new HashMap<>(consumersOffsetMap);
    }

    public void updateConsumersOffsetMap(HashMap<ConsumerOffsetInfo, Integer> consumersOffsetMap) {
        this.consumersOffsetMap.putAll(consumersOffsetMap);
    }

    public void addConsumerOffset(ConsumerOffsetInfo offsetInfo,int offset){
        consumersOffsetMap.put(offsetInfo,offset);
    }

    public void removeConsumerOffset(ConsumerOffsetInfo offsetInfo){
        consumersOffsetMap.remove(offsetInfo);
    }

    public void setRecords(String topic, List<Record> records) {
        recordsMap.put(topic, records);
    }

    public List<Record> getRecords(String topic) {
        return recordsMap.get(topic);
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

    public HashMap<String, RequestHeartbeat> getConsumerHeartbeat() {
        return new HashMap<>(consumerHeartbeat);
    }

    public void removeHeartbeat(String consumerId) {
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
