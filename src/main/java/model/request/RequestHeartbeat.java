package model.request;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.Serializable;

public class RequestHeartbeat implements Serializable {

    private final String consumerId;
    private final String groupId;
    private final int sessionTimeout;
    private final long heartbeatTime;

    public RequestHeartbeat(String groupId, String consumerId, int sessionId, long heartbeatTime) {
        this.groupId = groupId;
        this.consumerId = consumerId;
        this.sessionTimeout = sessionId;
        this.heartbeatTime = heartbeatTime;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public String getGroupId() {
        return groupId;
    }

    public int getSessionTimeout() {
        return sessionTimeout;
    }

    public long getHeartbeatTime() {
        return heartbeatTime;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
    }
}
