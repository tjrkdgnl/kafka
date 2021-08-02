package model.response;

import model.ProducerRecord;
import model.Topic;

import java.io.Serializable;

/***
 * 토픽이 존재하는지 확인 후 리턴되는 response data class
 *
 * @Variable topicData: broker에서 관리하고 있는 topic Data. record로부터 토픽이 존재하느지 확인하여
 * broker가 갖고 있는 topic을 전달하는데 사용되는 변수다. 이 변수가 현재 사용되는 목적은 partitioner의 round-robin에
 * 사용되는 총 partitions의 개수가 얼마인지 얻기 위해서다
 *
 * @Variable record: 클라이언트가 요청한 record. netty의 특성으로 outBound로부터 전송된 record를 inBoundHandler가
 * 알 수 있어야 하기때문에 클라이언트가 요청한 record도 같이 전송한다
 */
public class ResponseTopicMetadata implements Serializable {

    private final Topic topicMetadata;

    public ResponseTopicMetadata(Topic topicMetadata) {
        this.topicMetadata = topicMetadata;

    }

    public Topic getTopicMetadata() {
        return topicMetadata;
    }

}
