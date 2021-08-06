package consumer;

import model.ConsumerGroup;
import model.TopicPartition;

import java.util.HashMap;

/**
 * 컨슈머 그룹 정보와 구독한 토픽의 파티션 정보, offset 정보등을 관리하는 class
 */
public class ConsumerMetadata {
    //자신이 속한 group에 대한 정보
    private ConsumerGroup groupInfo;

    //토픽에 대한 offset을 관리
    private final HashMap<TopicPartition,Integer> topicPartitionAndOffset;

    public ConsumerMetadata(){
        topicPartitionAndOffset = new HashMap<>();
    }

    public void setGroupInfo(ConsumerGroup groupInfo) {
        this.groupInfo = groupInfo;
    }

    public ConsumerGroup getGroupInfo() {
        return groupInfo;
    }

    public HashMap<TopicPartition, Integer> getTopicPartitionAndOffset() {
        return topicPartitionAndOffset;
    }
}
