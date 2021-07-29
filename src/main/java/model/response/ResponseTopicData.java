package model.response;

import model.Topic;

import java.io.Serializable;
import java.util.List;

public class ResponseTopicData implements Serializable {

    private List<Topic> topicList;

    public ResponseTopicData(List<Topic> topic) {
        this.topicList = topic;
    }

    public List<Topic> getTopicList() {
        return topicList;
    }

}
