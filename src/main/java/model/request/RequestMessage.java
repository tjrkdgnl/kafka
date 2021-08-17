package model.request;

import model.StatusHeader;
import util.MemberState;

import java.io.Serializable;

public class RequestMessage extends StatusHeader implements Serializable {
    private int rebalanceId;
    private String[] topics;
    private String consumerId;
    private String groupId;

    public RequestMessage(MemberState status, String groupId) {
        super(status);
        this.groupId = groupId;
    }

    public RequestMessage(MemberState status, String groupId, String consumerId) {
        this(status, groupId);
        this.consumerId = consumerId;
    }

    public RequestMessage(MemberState status, int rebalanceId, String[] topics, String consumerId, String groupId) {
        this(status, groupId, consumerId);
        this.rebalanceId = rebalanceId;
        this.topics = topics;
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
