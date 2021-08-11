package model.request;

import model.StatusHeader;
import util.ConsumerRequestStatus;

import java.io.Serializable;

public class RequestPollingMessage extends StatusHeader implements Serializable {
    private final int rebalanceId;
    private final String[] topics;
    private final String consumerId;
    private final String groupId;

    public RequestPollingMessage(ConsumerRequestStatus status, int rebalanceId, String[] topics, String consumerId, String groupId){
        super(status);
        this.rebalanceId =rebalanceId;
        this.topics =topics;
        this.consumerId =consumerId;
        this.groupId =groupId;
    }

    public int getRebalanceId() {
        return rebalanceId;
    }

    public String[] getTopics() {
        return topics;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public String getGroupId() {
        return groupId;
    }
}
