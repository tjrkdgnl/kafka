package model.response;

import java.io.Serializable;

public class ResponseError implements Serializable {
    private final int status;
    private final String message;

    public ResponseError(int status,String message){
        this.status = status;
        this.message =message;
    }

    public int getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }
}
