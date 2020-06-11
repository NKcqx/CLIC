package channel;

import basic.Operators.Operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @description：为{@link Operator}管理数据的存储位置、IO等工作
 */
public class Channel {

    private Operator source_operator; // 边的起始点
    private Operator target_operator; // 边的终点

    private Map<String, String> key_pair; // source输出数据的key - target输入数据的key, 即source输出的值 传给 target输入数据的值

    public Channel(Operator source, Operator target, Map<String, String> key_pair){
        this.source_operator = source;
        this.target_operator = target;
        this.key_pair = key_pair;
    }

    public Channel(Operator source, List<String> output_keyset, Operator target, List<String> input_keyset) {
        source_operator = source;
        target_operator = target;
    }

    public Map<String, String> getKeyPair(){return this.key_pair;}

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

    public String retriveData(String key){
        return source_operator.getOutputData(key);
    }

    public Operator getTargetOperator(){
        return this.target_operator;
    }

}
