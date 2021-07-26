package manager;

import io.netty.channel.ChannelHandlerContext;
import model.ProducerRecord;
import model.response.ResponseTopicData;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;

public class FileManager {

    private FileManager() {

    }


    public static FileManager getInstance() {
        return FileSingletonHolder.fileManager;
    }


    public void readTopicMetaData(ChannelHandlerContext ctx) throws IOException {
        ctx.executor().execute(() -> {
            AsynchronousFileChannel fileChannel = null;
            try {
                fileChannel = AsynchronousFileChannel.open(Path.of("/Users/user/IdeaProjects/2021_SeoKangHwi/data/topicMetadata"),
                        StandardOpenOption.READ);

                long fileSize = fileChannel.size();

                ByteBuffer buffer = ByteBuffer.allocate((int) fileSize);

                fileChannel.read(buffer, 0, null, new CompletionHandler<Integer, Object>() {
                    @Override
                    public void completed(Integer result, Object attachment) {
                        if (result == -1) {
                            FileSingletonHolder.logger.error("file result is error : value : -1");
                            return;
                        }

                        buffer.flip();
                        buffer.mark();
                        String[] metadata = new String(buffer.array()).split(" ");

                        int size = -1;

                        if (StringUtils.isNumeric(metadata[1].trim())) {
                            size = Integer.parseInt(metadata[1].trim());
                            ArrayList<Integer> lst = new ArrayList<>();
                            for (int i = 0; i < size; i++) {
                                lst.add(i);
                            }
                            ctx.channel().writeAndFlush(new ResponseTopicData(metadata[0].trim(), lst));
                            FileSingletonHolder.logger.info("async file 완료 \n");
                            return;
                        }

                        //error가 나는 경우 핸들링하기
                    }

                    @Override
                    public void failed(Throwable exc, Object attachment) {
                        FileSingletonHolder.logger.trace(exc.getStackTrace());
                    }
                });
            } catch (IOException e) {
                e.printStackTrace();
            }

        });
    }


    public void writeTopicMetaData(ChannelHandlerContext ctx, ProducerRecord record) throws IOException {
        ctx.executor().execute(() -> {
            AsynchronousFileChannel fileChannel = null;
            try {
                fileChannel = AsynchronousFileChannel.open(Path.of("/Users/user/IdeaProjects/2021_SeoKangHwi/data/topic-log-" + record.getPartition()),
                        StandardOpenOption.WRITE);

                ByteBuffer buffer =ByteBuffer.allocate(1024);

                buffer.put(record.getValue().getBytes(StandardCharsets.UTF_8));
                buffer.flip();

                fileChannel.write(buffer, 0, null, new CompletionHandler<Integer, Object>() {
                    @Override
                    public void completed(Integer result, Object attachment) {


                    }

                    @Override
                    public void failed(Throwable exc, Object attachment) {
                    }
                });


            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }


    private static class FileSingletonHolder {
        private static final FileManager fileManager = new FileManager();

        private static final Logger logger = Logger.getLogger(FileManager.class);
    }
}
