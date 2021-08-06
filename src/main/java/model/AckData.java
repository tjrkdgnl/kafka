package model;

import java.io.Serializable;

public class AckData implements Serializable {

    int status;
    String message;

    public AckData(int status,String message){
        this.status =status;
        this.message =message;
    }

    public int getStatus(){
        return status;
    }

    public String getMessage(){
        return message;
    }

}
