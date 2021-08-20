package model.request;

import model.ConsumerOffsetInfo;

import java.io.Serializable;
import java.util.HashMap;

public class RequestCommit implements Serializable {
    private final String consumerId;
    private final HashMap<ConsumerOffsetInfo, Integer> offsetInfo;

    public RequestCommit(String consumerId,HashMap<ConsumerOffsetInfo, Integer> offsetInfo) {
        this.consumerId =consumerId;
        this.offsetInfo = offsetInfo;

    }

    public String getConsumerId() {
        return consumerId;
    }

    public HashMap<ConsumerOffsetInfo, Integer> getOffsetInfo() {
        return offsetInfo;
    }
}
