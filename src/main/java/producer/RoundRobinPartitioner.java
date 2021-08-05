package producer;

import com.sangupta.murmur.Murmur2;
import util.DataUtil;

import java.nio.charset.StandardCharsets;

public class RoundRobinPartitioner {

    public int partition(int partitionCount) throws Exception {
        String timestamp = DataUtil.createTimestamp();

        byte[] bytes = timestamp.getBytes(StandardCharsets.UTF_8);

        return (int) (Murmur2.hash(bytes, bytes.length, partitionCount) % partitionCount);
    }


}
