package model.response;

import model.ConsumerGroup;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.Serializable;

public class UpdateGroupInfo implements Serializable {
    private ConsumerGroup consumerGroup;

    public UpdateGroupInfo() {

    }

    public UpdateGroupInfo(ConsumerGroup consumerGroup) {

        this.consumerGroup = consumerGroup;
    }


    public ConsumerGroup getConsumerGroup() {
        return consumerGroup;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
    }
}
