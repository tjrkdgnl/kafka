package model;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.Serializable;

/**
 * producer가 publish한 record 객체로 broker에게 전달되는 request Object
 *
 * @Variable topic: topic name
 * @Variable key: key of topic partition
 * @Variable value: publish message from producer
 * @Variable partitionId
 * - key가 존재하는 경우: key를 int로 형변환 후 id값으로 셋팅
 * - key가 존재하지 않는 경우: Round-robin Partitioner로부터 id값을 셋팅
 */
public class ProducerRecord implements Serializable {
    private String topic;
    private Integer partition;
    private String value;


    public ProducerRecord(String topic,String value){
        this.topic =topic;
        this.partition=null;
        this.value=value;
    }

    public ProducerRecord(String topic, Integer partition, String value) {
        this(topic, value);
        this.partition = partition;
    }

    public String getValue() {
        return value;
    }

    public String getTopic() {
        return topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(int partition){
        this.partition =partition;
    }


    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this,ToStringStyle.JSON_STYLE);
    }
}
