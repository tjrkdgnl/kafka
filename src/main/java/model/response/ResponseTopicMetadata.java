package model.response;

import model.Topic;

import java.io.Serializable;

/***
 * 토픽이 존재하는지 확인 후 리턴되는 response data class
 *
 * @Variable topicData: broker에서 관리하고 있는 topic Data. record로부터 토픽이 존재하느지 확인하여
 * broker가 갖고 있는 topic을 전달하는데 사용되는 변수다. 이 변수가 현재 사용되는 목적은 partitioner의 round-robin에
 * 사용되는 총 partitions의 개수가 얼마인지 얻기 위해서다
 *
 *  @Variable request_id: 요청한 client의 id를 의미
 */
public class ResponseTopicMetadata implements Serializable {
    private final String request_id;
    private final Topic topicMetadata;

    public ResponseTopicMetadata(String request_id,Topic topicMetadata) {
        this.request_id = request_id;
        this.topicMetadata = topicMetadata;

    }

    public String getRequest_id() {
        return request_id;
    }

    public Topic getTopicMetadata() {
        return topicMetadata;
    }

}
