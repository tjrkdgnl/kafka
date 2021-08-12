package model.response;

import model.ConsumerGroup;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import util.GroupStatus;

import java.io.Serializable;

public class UpdateGroupInfo implements Serializable {
    private GroupStatus groupStatus;
    private ConsumerGroup consumerGroup;

    public UpdateGroupInfo() {

    }

    public UpdateGroupInfo(GroupStatus status){
        this.groupStatus =status;
    }

    public UpdateGroupInfo(GroupStatus status,ConsumerGroup consumerGroup) {
        this.groupStatus =status;
        this.consumerGroup = consumerGroup;
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
