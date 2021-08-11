package model;

import util.GroupStatus;

public class StatusHeader {
    private GroupStatus status;

    public StatusHeader(GroupStatus status){
        this.status =status;
    }

    public GroupStatus getStatus() {
        return status;
    }
}
