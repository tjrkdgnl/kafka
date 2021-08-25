package model.schema;

import model.OffsetData;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.ArrayList;
import java.util.List;

/**
 * offsetData를 리스트형태로 저장하기 위한 class
 */
public class Offsets {

    private List<OffsetData> offsetDataList;

    public Offsets(){
        offsetDataList =new ArrayList<>();
    }

    public List<OffsetData> getOffsetDataList() {
        return offsetDataList;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
    }
}
