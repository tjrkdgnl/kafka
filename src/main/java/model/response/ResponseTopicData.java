package model.response;

import java.io.Serializable;
import java.util.List;

public class ResponseTopicData implements Serializable {

    private String topic;
    private List<Integer> partitions;

    public ResponseTopicData(String topic, List<Integer> partition) {
        this.topic = topic;
        this.partitions = partition;
    }


    public String getTopic() {
        return topic;
    }

    public List<Integer> getPartitions() {
        return partitions;
    }
}
