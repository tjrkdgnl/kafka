package manager;

import model.Topic;

import java.util.List;

/***
 * topic List를 관리하는 manager class
 *
 * broker server로부터 topic과 partition 개수를 얻어온다
 *
 * @Variable topicList : Producer App 실행 시, Controller로부터 얻는 topic list
 */
public class TopicManager {
    private List<Topic> topicList;

    private TopicManager(){

    }

    public static TopicManager getInstance() {
        return SingletonTopicManager.topicManager;
    }

    public List<Topic> getTopicList(){
        return topicList;
    }

    public void setTopicList(List<Topic> topicList){
        this.topicList =topicList;
    }

    private static class SingletonTopicManager {
        private static final TopicManager topicManager = new TopicManager();
    }
}
