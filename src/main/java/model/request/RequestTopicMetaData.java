package model.request;

import model.ProducerRecord;

import java.io.Serializable;

public class RequestTopicMetaData implements Serializable {

    private ProducerRecord record;

    public RequestTopicMetaData(ProducerRecord record){
        this.record = record;
    }


    public ProducerRecord producerRecord(){
        return record;
    }

}
