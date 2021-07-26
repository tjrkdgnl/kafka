package util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * 컴포넌트간 주고받는 데이터를 object -> Bytebuf or ByteBuf -> object로 변환시켜주는 class
 */

public class DataUtil {

    public static ByteBuf parsingObjectToByteBuf(Object obj) throws Exception {

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);

        oos.writeObject(obj);

        byte[] buf = bos.toByteArray();

        ByteBuf byteBuf = Unpooled.directBuffer();
        byteBuf.writeBytes(buf);

        return byteBuf;
    }


    public static Object parsingBufToObject(ByteBuf buf) throws Exception {
        int length = buf.readableBytes();
        byte[] bytes = new byte[length];

        for(int i = 0; i< length; i ++){
            bytes[i] = buf.getByte(i);
        }

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bis);

        return ois.readObject();
    }
}
