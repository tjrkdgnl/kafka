package model.request;

import java.io.Serializable;

public class RequestConsumerGroup implements Serializable {

    private final String group_id;

    public RequestConsumerGroup(String group_id){
        this.group_id =group_id;
    }

    public String getGroup_id() {
        return group_id;
    }
}
