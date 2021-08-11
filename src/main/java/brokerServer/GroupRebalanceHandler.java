package brokerServer;

import model.ConsumerGroup;
import model.Topic;
import model.TopicPartition;
import model.Topics;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class GroupRebalanceHandler {

    public GroupRebalanceHandler() {

    }

    public void runRebalance(CompletableFuture<Boolean> resultFuture, ConsumerGroup consumerGroup) {

        try {
            //consumer들이 구독한 토픽들을 가져온다
            Set<String> subscriptionTopics = consumerGroup.getTopicMap().keySet();

            //현재 broker에서 관리하고있는 topic list
            Topics topics = BrokerServer.topics;

            //구독하려는 topic이 존재하는 topic인지 확인한다
            for (String subscribedTopic : subscriptionTopics) {
                for (Topic topic : topics.getTopicList()) {
                    if (topic.getTopic().equals(subscribedTopic)) {

                        int consumerIdx = 0;

                        //해당 토픽을 구독하려는 consumer list를 불러온다
                        List<String> consumerList = consumerGroup.getTopicMap().get(subscribedTopic);

                        //모든 partition의 ownership을 초기화 한다
                        consumerGroup.initOwnership();

                        //토픽의 partition을 consumer들에게 분배한다
                        for (int partition = 0; partition < topic.getPartitions(); partition++) {

                            //consumer와 ownership을 갖을 topicPartition들을 위해 list를 불러온다
                            List<TopicPartition> topicPartitions = consumerGroup.getOwnershipMap().getOrDefault(consumerList.get(consumerIdx), new ArrayList<>());

                            TopicPartition topicPartition = new TopicPartition(subscribedTopic, partition);
                            topicPartitions.add(topicPartition);

                            //consumer와 토픽의 파티션을 맵핑하고 저장한다
                            consumerGroup.addOwnership(consumerList.get(consumerIdx++), topicPartitions);

                            //첫 consumer부터 다시 topic의 파티션을 할당하기 위해 index를 초기화 한다
                            if (consumerIdx >= consumerList.size()) {
                                consumerIdx %= consumerList.size();
                            }
                        }
                    }
                }
            }

            resultFuture.complete(true);

        } catch (Exception e) {
            resultFuture.complete(false);
        }
    }
}
