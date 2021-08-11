package model.response;

import model.ConsumerGroup;
import model.StatusHeader;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import util.GroupStatus;

import java.io.Serializable;

public class ResponseGroupInfo  implements Serializable {
    private ConsumerGroup consumerGroup;

    public ResponseGroupInfo() {

    }

    public ResponseGroupInfo( ConsumerGroup consumerGroup) {

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
