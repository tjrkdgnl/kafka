package model;

import java.util.HashMap;

public class ConsumerGroups {
    private final HashMap<String, ConsumerGroup> groupInfoMap;

    public ConsumerGroups(){
        groupInfoMap = new HashMap<>();
    }

    public ConsumerGroups(HashMap<String, ConsumerGroup> groupInfoHashMap){
        this.groupInfoMap =groupInfoHashMap;
    }

    public HashMap<String, ConsumerGroup> getGroupInfoMap() {
        return groupInfoMap;
    }
}
