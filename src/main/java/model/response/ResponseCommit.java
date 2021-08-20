package model.response;

import java.io.Serializable;

public class ResponseCommit implements Serializable {
    private String consumerId;
    private int status;

    public ResponseCommit(String consumerId,int status){
        this.consumerId = consumerId;
        this.status =status;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public int getStatus() {
        return status;
    }
}
