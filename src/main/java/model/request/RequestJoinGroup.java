package model.request;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;

public class RequestJoinGroup implements Serializable {

    private final String group_id;
    private final String consumer_id;
    private final String[] topics;

    public RequestJoinGroup(String consumerGroup, String consumer_id , String[] subscriptions){
        this.group_id = consumerGroup;
        this.consumer_id = consumer_id;
        this.topics =subscriptions;
    }

    public String[] getTopics() {
        return topics;
    }

    public String getConsumer_id() {
        return consumer_id;
    }

    public String getGroup_id() {
        return group_id;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
