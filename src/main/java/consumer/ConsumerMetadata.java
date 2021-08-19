package consumer;

import model.TopicPartition;
import util.MemberState;

import java.util.HashMap;
import java.util.List;

/**
 * 컨슈머 그룹 정보와 구독한 토픽의 파티션 정보, offset 정보등을 관리하는 class
 */
public class ConsumerMetadata {
    //토픽에 대한 offset을 관리
    private final HashMap<TopicPartition, Integer> topicPartitionAndOffset;
    private int rebalanceId;
    private MemberState status;

    public ConsumerMetadata() {
        topicPartitionAndOffset = new HashMap<>();
        rebalanceId = 0;
        status = MemberState.JOIN;
    }

    public int getRebalanceId() {
        return rebalanceId;
    }

    public void setRebalanceId(int rebalanceId) {
        this.rebalanceId = rebalanceId;
    }

    public int getOffsetOfTopicPartition(TopicPartition topicPartition) {
        return topicPartitionAndOffset.get(topicPartition);
    }

    public void setTopicPartitions(List<TopicPartition> topicPartitions) {
        for (TopicPartition topicPartition : topicPartitions) {
            this.topicPartitionAndOffset.put(topicPartition, 0);
        }
    }

    public void updateTopicPartition(int offset, TopicPartition topicPartition) {
        topicPartitionAndOffset.put(topicPartition, offset);
    }

    public HashMap<TopicPartition, Integer> getTopicPartitionAndOffset() {
        return topicPartitionAndOffset;
    }

    public MemberState getStatus() {
        return status;
    }

    public void setStatus(MemberState status) {
        this.status = status;
    }
}
