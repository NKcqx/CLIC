package fdu.daslab.executable.basic.serialize;

import java.io.Serializable;

/**
 * 序列化工具类，可支持多种序列化方式
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/9/25 10:12 AM
 */
public class SerializeUtil implements Serializable {

    /**
     * 序列化
     *
     * @param serializeMethod 序列化的方式
     * @param obj 需要序列化的对象
     * @return 字节数据
     */
    public static byte[] serialize(String serializeMethod, Object obj) {
        if ("java".equals(serializeMethod)) {
            return JavaSerializeUtil.serialize(obj);
        }
        return new byte[]{};
    }

    /**
     * 反序列化
     *
     * @param deserializeMethod 反序列化的方式
     * @param bytes 需要反序列化的字节数据
     * @param len 长度
     * @return 序列化对象
     */
    public static Object deserialize(String deserializeMethod, byte[] bytes, int len) {
        if ("java".equals(deserializeMethod)) {
            return JavaSerializeUtil.deserialize(bytes, len);
        }
        return null;
    }
}
