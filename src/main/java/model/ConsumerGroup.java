package model;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

public class ConsumerGroup implements Serializable {
    private String group_id;

    //consumer가 갖고 있는 topic의 정보
    private final HashMap<String, List<TopicPartition>> ownershipMap;

    public ConsumerGroup(){
        ownershipMap = new HashMap<>();
    }

    public void setGroup_id(String group_id) {
        this.group_id = group_id;
    }

    public String getGroup_id() {
        return group_id;
    }

    public HashMap<String, List<TopicPartition>> getOwnershipMap() {
        return ownershipMap;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
    }
}
