package model;


import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Controller로부터 저장된 Topic MeataData class
 *
 * @Variable topic: topic name
 * @Variable partitions: 해당 토픽의 총 파티션 개수
 * @Variable partitionLeaderMap: 브로커와 브로커가 리더인 partition을 mapping한 HashMap
 * - key : Broker ID
 * - value: partition number
 */
public class Topic implements Serializable {
    private String topic;
    private int partitions;
    private HashMap<String,Integer> partitionLeaderMap;

    public Topic(){

    }

    public Topic(String topic,int partitions){
        this.topic = topic;
        this.partitions =partitions;
    }

    public Topic(String topic,int partitions,String leaderBrokerId,int partitionOfLeader){
        this.topic = topic;
        this.partitions =partitions;
        this.partitionLeaderMap =new HashMap<>();
        this.partitionLeaderMap.put(leaderBrokerId,partitionOfLeader);
    }


    public String getTopic(){
        return topic;
    }

    public int getPartitions(){
        return partitions;
    }

    public int getLeaderPartition(String brokerId){
        return partitionLeaderMap.get(brokerId);
    }

    public HashMap<String, Integer> getPartitionLeaderMap() {
        return partitionLeaderMap;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
