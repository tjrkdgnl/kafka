package model;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.Serializable;
import java.util.Objects;

public class ConsumerRecord implements Serializable {
    private TopicPartition topicPartition;
    private int offset;
    private String message;

    public ConsumerRecord(TopicPartition topicPartition, int offset, String message) {
        this.topicPartition =topicPartition;
        this.offset = offset;
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    public int getOffset() {
        return offset;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConsumerRecord that = (ConsumerRecord) o;
        return offset == that.offset && Objects.equals(topicPartition, that.topicPartition) && Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicPartition, offset, message);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
    }
}
