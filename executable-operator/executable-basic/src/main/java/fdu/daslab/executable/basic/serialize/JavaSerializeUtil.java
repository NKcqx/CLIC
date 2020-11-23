package fdu.daslab.executable.basic.serialize;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * java原生支持的序列化方式
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/9/23 6:45 PM
 */
public class JavaSerializeUtil implements Serializable {

    private static Logger logger = LoggerFactory.getLogger(JavaSerializeUtil.class);

    // 序列化对象
    public static byte[] serialize(Object obj) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(out);
            objectOutputStream.writeObject(obj);
            return out.toByteArray();
        } catch (Exception e) {
            logger.error("JavaSerialize fail:" + e.getMessage());
        }
        return new byte[]{};
    }

    // 反序列化对象
    public static Object deserialize(byte[] bytes, int len) {
        try {
            ObjectInputStream objectInputStream = new ObjectInputStream(
                    new ByteArrayInputStream(bytes, 0, len));
            return objectInputStream.readObject();
        } catch (Exception e) {
            logger.error("JavaDeserialize fail:" + e.getMessage());
        }
        return null;
    }
}
