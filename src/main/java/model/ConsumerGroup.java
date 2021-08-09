package model;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

public class ConsumerGroup implements Serializable {
    private String group_id;

    //member가 갖고 있는 topic의 정보
    private HashMap<String, List<TopicPartition>> ownershipMap;

    private HashMap<String,List<String>> topicMap;

    public ConsumerGroup(){
        ownershipMap = new HashMap<>();
        topicMap = new HashMap<>();
    }

    public ConsumerGroup(String group_id, HashMap<String,List<TopicPartition>> memberTopicPartitions){
        this.group_id =group_id;
        this.ownershipMap =memberTopicPartitions;
    }

    public void setGroup_id(String group_id) {
        this.group_id = group_id;
    }

    public String getGroup_id() {
        return group_id;
    }

    public HashMap<String, List<String>> getTopicMap() {
        return topicMap;
    }

    public void setOwnershipMap(HashMap<String, List<TopicPartition>> ownershipMap) {
        this.ownershipMap = ownershipMap;
    }

    public HashMap<String, List<TopicPartition>> getOwnershipMap() {
        return ownershipMap;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
    }
}
