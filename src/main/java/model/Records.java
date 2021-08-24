package model;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Records implements Serializable {

    private List<RecordData> recordData;

    public Records(){
        recordData = new ArrayList<>();
    }

    public List<RecordData> getRecords() {
        return recordData;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
    }
}
