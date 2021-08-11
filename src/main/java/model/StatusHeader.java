package model;

import util.ConsumerRequestStatus;

import java.io.Serializable;

public class StatusHeader implements Serializable {
    private ConsumerRequestStatus status;

    public StatusHeader(){

    }

    public StatusHeader(ConsumerRequestStatus status){
        this.status =status;
    }

    public ConsumerRequestStatus getStatus() {
        return status;
    }
}
