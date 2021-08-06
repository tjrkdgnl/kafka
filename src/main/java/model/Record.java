package model;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;


/**
 * producerRecord를 의미
 *
 *
 * @variable topic: topic name
 * @variable message: 작성된 메세지
 * @variable partition: topic의 파티션
 * @variable offset: relativeOffset. 해당 offset을 통해 consume하는데 소비
 */
public class Record implements Serializable {
    String topic;
    String message;
    int partition;
    int offset;

    public Record(){

    }

    public Record(String topic,String message,int partition,int offset){
        this.topic =topic;
        this.message =message;
        this.partition =partition;
        this.offset =offset;
    }

    public String getTopic() {
        return topic;
    }

    public String getMessage() {
        return message;
    }

    public int getPartition() {
        return partition;
    }

    public int getOffset() {
        return offset;
    }

    public int size(){
        //8은 총 partition, offset 두 int 타입의 합
        return topic.length()+message.length()+ 8;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
