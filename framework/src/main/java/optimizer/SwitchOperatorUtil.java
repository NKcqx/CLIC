package optimizer;

import basic.operators.Operator;
import channel.Channel;

/**
 * 交换算子需要的工具类
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/7/22 2:43 PM
 */
public class SwitchOperatorUtil {
    /**
     * 连接两个operator，可重复
     *
     * @param source source算子
     * @param target target算子
     */
    public static void connectTwoOperator(Operator source, Operator target) {
        // 构造channel
        Channel channel = new Channel(source, target, null);
        source.connectTo(channel);
        target.connectFrom(channel);
    }

    public static void connectTwoOperatorAlone(Operator source, Operator target) {
        source.cleanOutputChannel();
        target.cleanInputChannel();
        connectTwoOperator(source, target);
    }
}
