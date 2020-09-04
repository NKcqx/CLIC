package channel;

import basic.operators.Operator;
import javafx.util.Pair;

import java.util.List;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/7/6 1:40 下午
 */
public class Channel {

    private Operator sourceOperator; // 边的起始点
    private Operator targetOperator; // 边的终点

    private Pair<String, String> keyPair; // source输出数据的key - target输入数据的key, 即source输出的值 传给 target输入数据的值

    public Channel(Operator source, Operator target, Pair<String, String> keyPair) {
        this.sourceOperator = source;
        this.targetOperator = target;
        this.keyPair = keyPair;
    }

    public Channel(Operator source, Operator target, String sourceKey, String targetKey) {
        this(source, target, new Pair<>(sourceKey, targetKey));
    }

    public Channel(Operator source, Operator target) throws Exception {
        if (source.getOutputDataList().size() != 1 || target.getInputDataList().size() != 1) {
            throw new Exception("source 或 target 具有多个输入输出，请指明要链接的Key Pair");
        }
        String sourceOutputKey = (String) source.getOutputDataList().keySet().toArray()[0];
        String targetInputKey = (String) target.getInputDataList().keySet().toArray()[0];
        this.sourceOperator = source;
        this.targetOperator = target;
        this.keyPair = new Pair<>(sourceOutputKey, targetInputKey);
    }

    public Channel(Operator source, List<String> outputKeySet, Operator target, List<String> inputKeySet) {
        sourceOperator = source;
        targetOperator = target;
    }

    public Pair<String, String> getKeyPair() {
        return this.keyPair;
    }

    public Operator getTargetOperator() {
        return this.targetOperator;
    }

    public Operator getSourceOperator() {
        return this.sourceOperator;
    }

}
