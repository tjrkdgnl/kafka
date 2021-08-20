package model.request;

import model.StatusHeader;
import model.TopicPartition;
import util.MemberState;

import java.io.Serializable;
import java.util.List;

public class RequestMessage extends StatusHeader implements Serializable {
    private int rebalanceId;
    private List<TopicPartition> subscriptions;
    private String consumerId;
    private String groupId;
    private int recordSize = 1;

    public RequestMessage(MemberState status, String groupId) {
        super(status);
        this.groupId = groupId;
    }

    public RequestMessage(MemberState status, String groupId, String consumerId) {
        this(status, groupId);
        this.consumerId = consumerId;
    }

    public RequestMessage(MemberState status, int rebalanceId, List<TopicPartition> subscriptions, String consumerId, String groupId) {
        this(status, groupId, consumerId);
        this.rebalanceId = rebalanceId;
        this.subscriptions = subscriptions;
    }

    public int getRecordSize() {
        return recordSize;
    }

    public int getRebalanceId() {
        return rebalanceId;
    }

    public List<TopicPartition> getSubscriptions() {
        return subscriptions;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public String getGroupId() {
        return groupId;
    }
}
