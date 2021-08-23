package consumer;

import model.ConsumerOffsetInfo;
import model.TopicPartition;
import org.apache.log4j.Logger;
import util.MemberState;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 컨슈머 그룹 정보와 구독한 토픽의 파티션 정보, offset 정보등을 관리하는 class
 */
public class ConsumerMetadata {
    private final Logger logger;
    //토픽에 대한 offset을 관리
    private final HashMap<TopicPartition, Integer> topicPartitionAndOffset;
    private int rebalanceId;
    private MemberState status;
    private HashMap<ConsumerOffsetInfo, Integer> uncommittedOffset;
    private int recordSize;

    public ConsumerMetadata() {
        logger = Logger.getLogger(ConsumerMetadata.class);
        topicPartitionAndOffset = new HashMap<>();
        rebalanceId = 0;
        status = MemberState.JOIN;
        uncommittedOffset = new HashMap<>();
    }

    public void setRecordSize(int recordSize) {
        this.recordSize = recordSize;
    }

    public int getRecordSize() {
        return recordSize;
    }

    private void clearUncommitedOffset() {
        uncommittedOffset = new HashMap<>();
    }

    public void setUncommittedOffset(HashMap<ConsumerOffsetInfo, Integer> uncommittedList) {
        this.uncommittedOffset = uncommittedList;
    }

    public HashMap<ConsumerOffsetInfo, Integer> getUncommittedOffset() {
        return new HashMap<>(uncommittedOffset);
    }

    public boolean checkUnCommitedOffset() {
        if (uncommittedOffset.size() != 0) {
            return false;
        } else {
            return true;
        }
    }

    public int getOffset(TopicPartition topicPartition) {
        return topicPartitionAndOffset.getOrDefault(topicPartition, 1);
    }

    public void updateTopicPartitionAndOffset() {
        for (Map.Entry<ConsumerOffsetInfo, Integer> offsetInfo : uncommittedOffset.entrySet()) {
            TopicPartition topicPartition = offsetInfo.getKey().getTopicPartition();
            topicPartitionAndOffset.put(topicPartition, offsetInfo.getValue());
        }

        clearUncommitedOffset();
    }

    public int getRebalanceId() {
        return rebalanceId;
    }

    public void setRebalanceId(int rebalanceId) {
        this.rebalanceId = rebalanceId;
    }

    public void setTopicPartitions(List<TopicPartition> topicPartitions) {
        for (TopicPartition topicPartition : topicPartitions) {
            this.topicPartitionAndOffset.put(topicPartition, 1);
        }
    }

    public MemberState getStatus() {
        return status;
    }

    public void setStatus(MemberState status) {
        this.status = status;
    }
}
