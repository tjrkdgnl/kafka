package brokerServer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import model.AckData;
import model.ProducerRecord;
import model.Topic;
import model.request.RequestTopicMetaData;
import model.response.ResponseTopicData;
import org.apache.log4j.Logger;
import util.DataUtil;
import util.ERROR;

import java.util.ArrayList;
import java.util.List;

public class BrokerInBoundHandler extends ChannelInboundHandlerAdapter {

    private final Logger logger = Logger.getLogger(BrokerInBoundHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        Object obj = DataUtil.parsingBufToObject((ByteBuf) msg);

        if (obj == null) {
            ctx.channel().writeAndFlush(new AckData(400, ERROR.OBJECT_NULL +
                    ": 브로커가 null Object를 받았습니다."));
        }
        else if (obj instanceof RequestTopicMetaData) {
            //ToDo Controller로부터 topicList를 얻어오는 로직 구현한 후, Response data 리턴하기
            logger.info("브로커가 클라이언트로부터 TopicMetadata를 요청받았습니다.");

            //file i/o를 구현하지 않았기 때문에 임시 값으로 지정하여 테스트
            List<Topic> topicList = new ArrayList<>();
            topicList.add(new Topic("daily-report-topic",3));

            ctx.channel().writeAndFlush(new ResponseTopicData(topicList));

        }
        else if (obj instanceof ProducerRecord) {
            //ToDo message 토픽 파티션에 파일로 저장하는 로직 동작된 후 Ack 전송하기
            logger.info("브로커가 프로듀서로부터 Record를 받았습니다.");

            ctx.channel().writeAndFlush(new AckData(200, "Success: 브로커가 정상적으로 Record를 받았습니다."));
        }
        else {
            ctx.channel().writeAndFlush(new AckData(400,  ERROR.TYPE_ERROR +
                    ": 브로커에서 알 수 없는 type의 object를 받았습니다."));
        }

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}
