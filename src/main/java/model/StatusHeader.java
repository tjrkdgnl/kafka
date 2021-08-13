package model;

import util.MemberState;

import java.io.Serializable;

public class StatusHeader implements Serializable {
    private MemberState status;

    public StatusHeader() {

    }

    public StatusHeader(MemberState status) {
        this.status = status;
    }

    public MemberState getStatus() {
        return status;
    }
}
