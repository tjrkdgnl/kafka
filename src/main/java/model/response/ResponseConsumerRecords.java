package model.response;

import model.ConsumerRecord;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.Serializable;
import java.util.List;

public class ResponseConsumerRecords implements Serializable {
    private String groupId;
    private String consumerId;
    private List<ConsumerRecord> consumerRecords;


    public ResponseConsumerRecords(String groupId,String consumerId,List<ConsumerRecord> consumerRecords){
        this.groupId =groupId;
        this.consumerId =consumerId;
        this.consumerRecords = consumerRecords;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public List<ConsumerRecord> getConsumerRecords() {
        return consumerRecords;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
    }
}
