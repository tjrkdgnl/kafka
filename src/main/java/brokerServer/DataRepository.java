package brokerServer;

import model.*;
import model.request.RequestHeartbeat;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DataRepository {
    private Topics topics;
    private final ConcurrentHashMap<String, List<String>> groups;
    private final Set<String> rebalancingGroup;
    //consumer-heartbeat
    private final ConcurrentHashMap<String, RequestHeartbeat> consumerHeartbeat;
    //ConsumerTopicInfo-offset
    private final ConcurrentHashMap<ConsumerOffsetInfo, Integer> consumersOffsetMap;
    //topic-records
    private final ConcurrentHashMap<TopicPartition, List<RecordData>> recordsMap;


    public DataRepository() {
        this.topics = new Topics();
        this.groups = new ConcurrentHashMap<>();
        this.consumerHeartbeat = new ConcurrentHashMap<>();
        this.rebalancingGroup = new HashSet<>();
        this.consumersOffsetMap = new ConcurrentHashMap<>();
        this.recordsMap = new ConcurrentHashMap<>();
    }

    public static DataRepository getInstance() {
        return SingletonRepo.INSTANCE;
    }

    public void setTopics(Topics topics) {
        this.topics = topics;
    }

    public void addTopic(Topic topic) {
        this.topics.getTopicList().add(topic);
    }

    public Topics getTopics() {
        return new Topics(topics.getTopicList());
    }

    public HashMap<ConsumerOffsetInfo, Integer> getConsumerOffsetMap() {
        return new HashMap<>(consumersOffsetMap);
    }

    public void updateConsumersOffsetMap(HashMap<ConsumerOffsetInfo, Integer> consumersOffsetMap) {
        this.consumersOffsetMap.putAll(consumersOffsetMap);
    }

    public void addConsumerOffset(ConsumerOffsetInfo offsetInfo, int offset) {
        consumersOffsetMap.put(offsetInfo, offset);
    }

    public void removeConsumerOffset(ConsumerOffsetInfo offsetInfo) {
        consumersOffsetMap.remove(offsetInfo);
    }

    public void setRecords(TopicPartition topicPartition, List<RecordData> recordData) {
        recordsMap.put(topicPartition, recordData);
    }

    public List<RecordData> getRecords(TopicPartition topicPartition) {
        return new ArrayList<>(recordsMap.get(topicPartition));
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
