package model.response;

import model.schema.ConsumerGroup;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import util.GroupStatus;

import java.io.Serializable;

public class UpdateGroupInfo implements Serializable {
    private final GroupStatus groupStatus;
    private final String consumerId;
    private ConsumerGroup consumerGroup;

    public UpdateGroupInfo(GroupStatus status, String consumerId) {
        this.consumerId = consumerId;
        this.groupStatus = status;
    }

    public UpdateGroupInfo(GroupStatus status, ConsumerGroup consumerGroup, String consumerId) {
        this.consumerId = consumerId;
        this.groupStatus = status;
        this.consumerGroup = consumerGroup;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public GroupStatus getGroupStatus() {
        return groupStatus;
    }

    public ConsumerGroup getConsumerGroup() {
        return consumerGroup;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
    }
}
