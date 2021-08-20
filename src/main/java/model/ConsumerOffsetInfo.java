package model;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.Serializable;
import java.util.Objects;

public class ConsumerOffsetInfo implements Serializable {
    private String consumerGroupId;
    private String consumerId;
    private TopicPartition topicPartition;

    public ConsumerOffsetInfo() {

    }

    public ConsumerOffsetInfo(String consumerGroupId, String consumerId, TopicPartition topicPartition) {
        this.consumerGroupId = consumerGroupId;
        this.consumerId = consumerId;
        this.topicPartition = topicPartition;
    }

    public String getConsumerGroupId() {
        return consumerGroupId;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConsumerOffsetInfo topicInfo = (ConsumerOffsetInfo) o;
        return Objects.equals(consumerGroupId, topicInfo.consumerGroupId) && Objects.equals(consumerId, topicInfo.consumerId) && Objects.equals(topicPartition, topicInfo.topicPartition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumerGroupId, consumerId, topicPartition);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
    }
}
