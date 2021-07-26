package manager;

import java.util.List;

/***
 * topic의 정보를 관리하는 manager class
 * broker server로부터 topic과 partition 개수를 얻어온다
 *
 * 현재 broker server를 개발하지 않았으므로 임시로 값을 셋팅하여 사용한다
 *
 * @Variable topic
 * - topic name
 *
 * @Variable partitionCount
 * - topic의 partition 총 개수
 */
public class TopicMetadata {
    private String topic ;
    private List<Integer> partitions;

    //싱글톤으로만 객체를 사용하기 위함
    private TopicMetadata(){

    }

    public static TopicMetadata newInstance(){
        return TopicSingletonHolder.topicManager;
    }

    public void setTopic(String topicName){
        TopicSingletonHolder.topicManager.topic =topicName;
    }

    public void setPartitions(List<Integer> partitions){
        TopicSingletonHolder.topicManager.partitions =partitions;
    }

    public String getTopic() {
        return  TopicSingletonHolder.topicManager.topic;
    }

    public List<Integer> getPartitions() {
        return  TopicSingletonHolder.topicManager.partitions;
    }


    public boolean checkPartition(Integer partition){

        if(partition == null)
            return false;

        for(int i = 0 ; i < partitions.size() ;i++){
            if(partitions.get(i).equals(partition))
                return true;
        }

        return false;
    }


    private static class TopicSingletonHolder {
        private static final TopicMetadata topicManager = new TopicMetadata();
    }
}
