package model;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * List 형태를 byte array로 변형하기 위한 avro topics schema
 */
public class Topics implements Serializable {

    List<Topic> topicList;

    public Topics(){
        this.topicList =new ArrayList<>();
    }

    public Topics(List<Topic> topics){
        this.topicList =topics;
    }

    public List<Topic> getTopicList() {
        return topicList;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
