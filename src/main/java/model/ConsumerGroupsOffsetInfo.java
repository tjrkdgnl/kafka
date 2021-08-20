package model;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.HashMap;

public class ConsumerGroupsOffsetInfo {
    private HashMap<ConsumerOffsetInfo, Integer> consumerOffsetMap;

    public ConsumerGroupsOffsetInfo(){

    }

    public ConsumerGroupsOffsetInfo(HashMap<ConsumerOffsetInfo,Integer> offsetInfo) {
        consumerOffsetMap = offsetInfo;
    }

    public HashMap<ConsumerOffsetInfo, Integer> getConsumerOffsetMap() {
        return consumerOffsetMap;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
    }
}
