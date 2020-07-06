/**

 */
package api;

import basic.operators.Operator;
import basic.operators.OperatorFactory;
import channel.Channel;

import java.util.Map;


/**
 * 相当于DAG图中的每个节点，节点内部保存实际的Operator，提供一系列API以供构造下一跳（可以有多个）
 *
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/7/6 1:40 下午
 */
public final class DataQuanta {
    private Operator operator;

    private DataQuanta(Operator operator) {
        this.operator = operator;
    }

    /**
     * 根据ability创建一个DataQuanta，并载入（无依赖）参数值
     *
     * @param ability Operator要具有的功能
     * @param params  Operator参数的值，K-V形式，可为空；注意，这传入的参数值只能是静态的值，例如最大迭代次数、是否倒序，而不是依赖上一跳的输出
     * @return 完整的DataQuanta
     * @throws Exception 一系列错误的结合，包括XML结构解析错误、文件找不到、传入的key和配置文件里的参数名对不上等
     */
    public static DataQuanta createInstance(String ability, Map<String, String> params) throws Exception {
        if (ability.equals("empty")) {
            return null;
        } else {
            // 先创建出符合要求的operator
            Operator opt = DataQuanta.createOperator(ability);
            if (params != null) {
                // 再设置静态的输入数据
                for (Map.Entry entry : params.entrySet()) {
                    String key = (String) entry.getKey();
                    String value = (String) entry.getValue();
                    opt.setData(key, value);
                }
            }
            DataQuanta dq = new DataQuanta(opt);
            return dq;
        }
    }

    /**
     * 功能单一，只是为了确保只有这一个地方提到了OperatorMapping的事
     *
     * @param operatorAbility opt应该具有的功能
     * @return 包含该opt的新的DataQuanta
     * @throws Exception
     */
    private static Operator createOperator(String operatorAbility) throws Exception {
        // 先根据功能创建对应的opt
        Operator opt = OperatorFactory.createOperator(operatorAbility);
        // 封装为DataQuanta
        return opt;
    }

    /**
     * 给this的opt添加一个新的上一跳opt
     *
     * @param incoming    上一跳opt
     * @param paramsPair 指定和上一跳Opt的输出的哪个数据的key链接，格式为 <incoming.output_key, this.input_key>；为null时默认拿到其所有的输出
     * @return 当前已链接的incoming channel的数量，即代表有多少个上一跳了
     */
    public int incoming(DataQuanta incoming, Map<String, String> paramsPair) {
        assert incoming != null : "上一跳不能为空";
        Channel channel = new Channel(
                incoming.getOperator(),
                this.getOperator(),
                paramsPair
        );
        // 双向绑定
        incoming.operator.connectTo(channel);
        int numIncoming = this.operator.connectFrom(channel); // 返回下一个input_idx，不知道能干什么用
        return numIncoming;
    }

    /**
     * 给this的opt添加新的输出opt
     *
     * @param outgoing    下一跳opt
     * @param paramsPair 指定和下一跳Opt所需输入的哪个数据的key链接，格式为 <this.output_key, outgoing.input_key>；为null时默认传出所有数据
     * @return 当前已链接的outgoing channel的数量，即代表有多少个下一跳了
     */
    public int outgoing(DataQuanta outgoing, Map<String, String> paramsPair) {
        assert outgoing != null : "下一跳不能为空";
        Channel channel = new Channel(
                this.getOperator(),
                outgoing.getOperator(),
                paramsPair
        );
        // 双向绑定
        outgoing.operator.connectFrom(channel);
        int numIncoming = this.operator.connectTo(channel); // 返回下一个input_idx，不知道能干什么用
        return numIncoming;
    }

    /**
     * 拿到DataQuanta所代表的Operator
     *
     * @return DataQuanta所代表的Operator
     */
    public Operator getOperator() {
        return operator;
    }

}
