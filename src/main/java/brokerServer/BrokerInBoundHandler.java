package brokerServer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import model.AckData;
import model.ProducerRecord;
import model.request.RequestTopicMetaData;
import org.apache.log4j.Logger;
import util.DataUtil;
import util.ERROR;
import util.ProducerRecordHandler;
import util.TopicMetadataHandler;

public class BrokerInBoundHandler extends ChannelInboundHandlerAdapter {

    private final Logger logger = Logger.getLogger(BrokerInBoundHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        try {
            Object obj = DataUtil.parsingBufToObject((ByteBuf) msg);

            if (obj == null) {
                ctx.channel().writeAndFlush(new AckData(400, ERROR.OBJECT_NULL +
                        ": 브로커가 null Object를 받았습니다."));
            }
            else if (obj instanceof RequestTopicMetaData) {
                logger.info("브로커가 클라이언트로부터 TopicMetadata를 요청받았습니다.");

                RequestTopicMetaData requestTopicMetaData = (RequestTopicMetaData) obj;

                //토픽 데이터를 읽어온 후 클라이언트에게 전송한다
               new TopicMetadataHandler(BrokerServer.properties)
                       .getTopicMetaData(ctx, requestTopicMetaData.producerRecord());

            }
            else if (obj instanceof ProducerRecord) {
                logger.info("브로커가 프로듀서로부터 Record를 받았습니다.");

                ProducerRecord record = (ProducerRecord) obj;

                //record를 작성한 후 client에게 전송한다
                new ProducerRecordHandler(ctx,record).saveProducerRecord();

            }
            else {
                ctx.channel().writeAndFlush(new AckData(400, ERROR.TYPE_ERROR +
                        ": 브로커에서 알 수 없는 type의 object를 받았습니다."));
            }

        } catch (Exception e) {
            logger.error("client로부터 받은 msg object를 parsing 하던 중, 문제가 발생했습니다.",e);
        }

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("InboundHandler error",cause);
        ctx.close();
    }
}
