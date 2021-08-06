package model;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.Serializable;
import java.util.Objects;

public class TopicPartition implements Serializable {
    private String topic;
    private int partition;
    private int hash;

    public TopicPartition() {

    }

    public TopicPartition(String topic, int partition) {
        this.partition = partition;
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }


    @Override
    public int hashCode() {
        if (this.hash != 0) {
            return this.hash;
        } else {
            int result = 1;
            result = 31 * result + this.partition;
            result = 31 * result + Objects.hashCode(this.topic);
            this.hash = result;
            return result;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null) {
            return false;
        } else if (this.getClass() != obj.getClass()) {
            return false;
        } else {
            TopicPartition topicPartition = (TopicPartition) obj;

            return partition == topicPartition.partition && Objects.equals(topic, topicPartition.topic);
        }
    }


    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
    }
}
