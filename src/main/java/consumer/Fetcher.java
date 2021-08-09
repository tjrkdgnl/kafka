package consumer;

import io.netty.channel.ChannelFuture;
import model.ConsumerGroup;
import model.request.RequestConsumerGroup;
import org.apache.log4j.Logger;

import java.util.concurrent.CompletableFuture;

public class Fetcher {
    private final Logger logger;
    private final ConsumerMetadata metadata;
    public static CompletableFuture<ConsumerGroup> groupFuture;

    public Fetcher(ConsumerMetadata consumerMetadata) {
        this.logger = Logger.getLogger(Fetcher.class);
        this.metadata = consumerMetadata;
    }


    public void updateConsumerGroup(CompletableFuture<Boolean> updateFuture, ChannelFuture cf, String groupId) throws Exception {
        groupFuture = new CompletableFuture<>();

        cf.channel().writeAndFlush(new RequestConsumerGroup(groupId));

        ConsumerGroup consumerGroup = groupFuture.get();

        if(consumerGroup !=null){
            this.metadata.setGroupInfo(consumerGroup);

            updateFuture.complete(true);
        }
        else{
            throw new NullPointerException("consumerGroup이 null입니다.");
        }
    }
}
