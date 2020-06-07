package api;

import basic.Operators.Operator;
import basic.Operators.OperatorFactory;
import channel.Channel;

import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * 相当于DAG图中的每个节点，节点内部保存实际的Operator，提供一系列API以供构造下一跳（可以有多个）
 */
public class DataQuanta {
    private Operator operator;

    public DataQuanta(Operator operator){
        this.operator = operator;
    }

    /**
     * 指定下一跳运算符的功能，并自动通过Channel连接
     * @param ability 下一跳Opt需要有的功能
     * @return 封装了下一跳的DataQuanta
     * @throws Exception XML解析错误、找不到指定配置文件
     */
    public DataQuanta then(String ability) throws Exception{
        if (ability.equals("empty")){
            return null; // TODO
        }else {
            Operator nextOpt = this.createOperator(ability);
            DataQuanta nextDataQuanta = new DataQuanta(nextOpt);
            this.acceptOutgoing(nextDataQuanta);
            return nextDataQuanta;
        }

    }

    public DataQuanta setCalculator(String ability) throws Exception {
        Operator opt = this.createOperator(ability);
        this.operator = opt;
        return this;
    }

    /**
     * 给this的opt添加一个新的输入opt
     * @param incoming 输入opt
     * @return input_channel列表的index，代表输入opt在this里放到哪里了
     */
    public int acceptIncoming(DataQuanta incoming){
        Channel channel = new Channel(
                incoming.getOperator(),
                incoming.getOperator().getNextOutputIndex(),
                this.getOperator(),
                this.getOperator().getNextInputIndex()
        );
        // 双向绑定
        incoming.operator.connectTo(channel);
        int next_idx = this.operator.connectFrom(channel); // 返回下一个input_idx，不知道能干什么用
        return next_idx;
    }

    public int acceptOutgoing(DataQuanta outgoing){
        Channel channel = new Channel(
                this.getOperator(),
                this.getOperator().getNextOutputIndex(),
                outgoing.getOperator(),
                outgoing.getOperator().getNextInputIndex()
        );
        // 双向绑定
        outgoing.operator.connectFrom(channel);
        int next_idx = this.operator.connectTo(channel); // 返回下一个input_idx，不知道能干什么用
        return next_idx;
    }

    public Operator getOperator() {
        return operator;
    }

    /**
     * 功能单一，只是为了确保只有这一个地方提到了OperatorMapping的事
     * @param operator_ability opt应该具有的功能
     * @return 包含该opt的新的DataQuanta
     * @throws Exception
     */
    private Operator createOperator(String operator_ability) throws Exception {
        // 先根据功能创建对应的opt
        Operator opt = OperatorFactory.createOperator(operator_ability);
        // 封装为DataQuanta
        return opt;
    }

}
