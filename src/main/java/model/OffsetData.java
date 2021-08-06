package model;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.Serializable;

/**
 * log파일의 구분과 record 구분을 짓기 위한 offset class
 *
 * @variable selfOffset : offset 파일을 생성 시, 다음번 offset을 가르키기 위한 self physical offset
 * @variable physicalOffset : Log 파일을 구분짓기 위한 physical offset
 * @variable relativeOffset : record를 구분짓기 위한 relative offset
 *
 */
public class OffsetData implements Serializable {
    private int selfOffset;
    private int physicalOffset;
    private int relativeOffset;

    //스키마 생성을 위한 default constructor 생성
    public OffsetData(){

    }


    public OffsetData(int selfOffset,int physicalOffset, int relativeOffset){
        this.selfOffset = selfOffset;
        this.relativeOffset = relativeOffset;
        this.physicalOffset =physicalOffset;
    }

    public void plusSelfOffset() {
        this.selfOffset++;
    }

    public int getSelfOffset() {
        return selfOffset;
    }

    public void plusPhysicalOffset() {
        this.physicalOffset ++;
    }

    public int getPhysicalOffset() {
        return physicalOffset;
    }

    public void setRelativeOffset(int relativeOffset) {
        this.relativeOffset = relativeOffset;
    }

    public int getRelativeOffset() {
        return relativeOffset;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.JSON_STYLE);
    }

    //int형 변수가 3개이므로 사이즈는 12
    public int size(){
        return 12;
    }
}
