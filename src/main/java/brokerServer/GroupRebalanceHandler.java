package brokerServer;

import model.*;
import model.request.RequestMessage;
import org.apache.log4j.Logger;
import util.RebalanceState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GroupRebalanceHandler {
    private final Logger logger;
    private final ConsumerGroupOffsetHandler consumerGroupOffsetHandler;
    private final DataRepository dataRepository;

    GroupRebalanceHandler() {
        this.logger = Logger.getLogger(GroupRebalanceHandler.class);
        this.consumerGroupOffsetHandler = new ConsumerGroupOffsetHandler(BrokerServer.getProperties());
        dataRepository = DataRepository.getInstance();
    }


    public void runRebalance(ConsumerGroup consumerGroup, RequestMessage message, RebalanceCallbackListener listener) throws Exception {
        try {
            consumerGroup.setGroupId(message.getGroupId());
            consumerGroup.setRebalanceId(consumerGroup.getRebalanceId() + 1);

            //리밸런스를 위해서 topic을 구독하는 consumer 리스트를 생성한다
            for (TopicPartition topicPartition : message.getSubscriptions()) {
                List<String> consumerList = consumerGroup.getTopicMap().getOrDefault(topicPartition.getTopic(), new ArrayList<>());

                //구체적인 topic과 partition을 구독한 경우
                if (topicPartition.getPartition() != -1) {
                    consumerGroup.addAssignedTopicPartition(message.getConsumerId(), topicPartition);
                }

                if (!consumerList.contains(message.getConsumerId())) {
                    consumerList.add(message.getConsumerId());
                }
                consumerGroup.setConsumerList(topicPartition.getTopic(), consumerList);
            }

            //consumer들이 구독한 토픽들을 가져온다
            Set<String> subscriptionTopics = consumerGroup.getTopicMap().keySet();

            //현재 broker에서 관리하고있는 topic list
            Topics topics = dataRepository.getTopics();

            //현재 맵핑관계를 초기화시킨다
            consumerGroup.initOwnership();

            //구독하려는 topic이 존재하는 topic인지 확인한다
            for (String subscribedTopic : subscriptionTopics) {
                for (Topic topic : topics.getTopicList()) {

                    if (topic.getTopic().equals(subscribedTopic)) {
                        int consumerIdx = 0;

                        //해당 토픽을 구독하려는 consumer list를 불러온다
                        List<String> consumersInGroup = consumerGroup.getTopicMap().get(subscribedTopic);

                        //토픽의 partition을 consumer들에게 분배한다
                        for (int partition = 0; partition < topic.getPartitions(); partition++) {
                            TopicPartition topicPartition = new TopicPartition(subscribedTopic, partition);

                            boolean checkTopicPartition = false;

                            //consumer와 이미 맵핑관계에 있는 topicPartition인지 확인
                            for (List<TopicPartition> topicPartitions : consumerGroup.getOwnershipMap().values()) {
                                if (topicPartitions.contains(topicPartition)) {
                                    checkTopicPartition = true;
                                    break;
                                }
                            }

                            if (checkTopicPartition) {
                                continue;
                            }

                            updateOffsetInfo(topicPartition, consumerGroup.getGroupId(), consumersInGroup.get(consumerIdx));

                            //consumer와 토픽의 파티션을 맵핑하고 저장한다
                            consumerGroup.addOwnership(consumersInGroup.get(consumerIdx++), topicPartition);

                            //첫 consumer부터 다시 topic의 파티션을 할당하기 위해 index를 초기화한다
                            if (consumerIdx >= consumersInGroup.size()) {
                                consumerIdx = 0;
                            }
                        }
                    }
                }
            }


            listener.setResult(RebalanceState.SUCCESS);

        } catch (Exception e) {
            logger.error("파티션 분배를 진행 중에 문제가 발생했습니다.", e);
            listener.setResult(RebalanceState.FAIL);
        }
    }

    public void runRebalanceForRemoving(ConsumerGroup consumerGroup, RebalanceCallbackListener listener) throws Exception {
        try {
            //consumer들이 구독한 토픽들을 가져온다
            Set<String> subscriptionTopics = consumerGroup.getTopicMap().keySet();

            Set<String> consumersOutGroup = consumerGroup.getOwnershipMap().keySet();

            //하트비트를 통해 확인된 컨슈머들
            List<String> runningConsumers = DataRepository.getInstance().getConsumers(consumerGroup.getGroupId());
            logger.info(runningConsumers);

            //컨슈머 제거
            runningConsumers.forEach(consumersOutGroup::remove);

            List<TopicPartition> remainingTopicPartitions = new ArrayList<>();

            for (String removedConsumer : consumersOutGroup) {
                remainingTopicPartitions.addAll(consumerGroup.getOwnershipMap().get(removedConsumer));
                consumerGroup.removeOwnership(removedConsumer);

                for (String topic : subscriptionTopics) {
                    List<String> consumers = consumerGroup.getTopicMap().get(topic);
                    consumers.remove(removedConsumer);
                    consumerGroup.updateTopicMap(topic, consumers);
                }
            }

            int consumerIdx = 0;
            List<String> currentConsumers = new ArrayList<>(consumerGroup.getOwnershipMap().keySet());

            for (TopicPartition removedTopicPartition : remainingTopicPartitions) {
                //ownerShip을 넘겨받는 consumer로 OffsetInfo를 변경시켜준다
                updateOffsetInfo(removedTopicPartition, consumerGroup.getGroupId(), currentConsumers.get(consumerIdx));

                //ownerShip 변경
                consumerGroup.addOwnership(currentConsumers.get(consumerIdx++), removedTopicPartition);

                if (consumerIdx >= currentConsumers.size()) {
                    consumerIdx = 0;
                }
            }

            //파일로 작성하여 저장한다
            consumerGroupOffsetHandler.updateConsumerOffset();
            consumerGroup.setRebalanceId(consumerGroup.getRebalanceId() + 1);
            listener.setResult(RebalanceState.SUCCESS);

        } catch (Exception e) {
            logger.error("파티션 분배를 진행 중에 문제가 발생했습니다.", e);
            listener.setResult(RebalanceState.FAIL);
        }
    }

    public void updateOffsetInfo(TopicPartition movedTopicPartition, String groupId, String runningConsumer) {
        ConsumerOffsetInfo newOffsetInfo = new ConsumerOffsetInfo(groupId, runningConsumer, movedTopicPartition);

        if (dataRepository.getConsumerOffsetMap().size() != 0) {
            consumerGroupOffsetHandler.readConsumersOffset(consumersOffset -> {
                if (consumersOffset != null) {
                    //현재 저장된 offsetInfo를 불러온다
                    for (Map.Entry<ConsumerOffsetInfo, Integer> offsetInfo : consumersOffset.getConsumerOffsetMap().entrySet()) {
                        if (offsetInfo.getKey().getTopicPartition().equals(movedTopicPartition)) {
                            ConsumerOffsetInfo removedOffsetInfo = offsetInfo.getKey();
                            dataRepository.addConsumerOffset(newOffsetInfo, offsetInfo.getValue());
                            dataRepository.removeConsumerOffset(removedOffsetInfo);
                            return;
                        }
                    }
                }
            });
        }
        consumerGroupOffsetHandler.updateConsumerOffset();
    }


    public interface RebalanceCallbackListener {
        void setResult(RebalanceState status) throws Exception;
    }
}
