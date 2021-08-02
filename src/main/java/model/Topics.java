package model;

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

    public List<Topic> getTopicList() {
        return topicList;
    }
}
