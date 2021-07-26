package producer;

import com.sangupta.murmur.Murmur2;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

public class RoundRobinPartitioner {

    public int partition(int partitionCount) throws Exception {
        String timestamp = createTimestamp();

        byte[] bytes = timestamp.getBytes(StandardCharsets.UTF_8);

        return (int)(Murmur2.hash(bytes, bytes.length, partitionCount) % partitionCount);
    }

    private String createTimestamp() throws ParseException {
        Date currentUTC = Calendar.getInstance().getTime();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss", Locale.getDefault());
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        return String.valueOf(simpleDateFormat.parse(simpleDateFormat.format(currentUTC)).getTime());
    }
}
