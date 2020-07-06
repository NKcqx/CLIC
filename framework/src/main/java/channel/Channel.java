/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/7/6 1:40 下午
 */
package channel;

import basic.operators.Operator;

import java.util.List;
import java.util.Map;

public class Channel {

    private Operator sourceOperator; // 边的起始点
    private Operator targetOperator; // 边的终点

    private Map<String, String> keyPair; // source输出数据的key - target输入数据的key, 即source输出的值 传给 target输入数据的值

    public Channel(Operator source, Operator target, Map<String, String> keyPair) {
        this.sourceOperator = source;
        this.targetOperator = target;
        this.keyPair = keyPair;
    }

    public Channel(Operator source, List<String> outputKeySet, Operator target, List<String> inputKeySet) {
        sourceOperator = source;
        targetOperator = target;
    }

    public Map<String, String> getKeyPair() {
        return this.keyPair;
    }

//    /**
//     * 从起点Opt把key代表的数据发送到终点Opt的指定Key中
//     */
//    public void transferData(){
//        for (Map.Entry entry : this.key_pair.entrySet()){
//            String source_key = (String) entry.getKey();
//            String target_key = (String) entry.getValue();
//            String source_value = source_operator.getOutputData(source_key);
//            target_operator.setInputData(target_key, source_value);
//        }
//    }

    public String retriveData(String key) {
        return sourceOperator.getOutputData(key);
    }

    public Operator getTargetOperator() {
        return this.targetOperator;
    }

    public Operator getSourceOperator() {
        return this.sourceOperator;
    }

}
