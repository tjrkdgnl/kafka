package model.request;

import java.io.Serializable;

public class RequestTopicMetaData implements Serializable {

    private final String request_id;
    private final String topic;


    public RequestTopicMetaData(String request_id, String topic) {
        this.request_id = request_id;
        this.topic = topic;
    }


    public String getRequest_id() {
        return request_id;
    }

    public String getTopic() {
        return topic;
    }

}
