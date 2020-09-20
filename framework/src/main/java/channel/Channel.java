package channel;

import org.javatuples.Pair;
import org.jgrapht.graph.DefaultWeightedEdge;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/7/6 1:40 下午
 */
public class Channel extends DefaultWeightedEdge {
    private List<Pair<String, String>> keyPairs; // source输出数据的key - target输入数据的key, 即source输出的值 传给 target输入数据的值

    public Channel() {
        this(new Pair<>("result", "data"));
    }

    public Channel(Pair<String, String> keyPairs) {
        this.keyPairs = new ArrayList<>();
        this.keyPairs.add(keyPairs);
    }

    public Channel(List<Pair<String, String>> keyPairs) {
        this.keyPairs = keyPairs;
    }

    public Channel(String sourceKey, String targetKey) {
        this(new Pair<>(sourceKey, targetKey));
    }

//    public Channel(Operator source, Operator target) throws Exception {
//
//        if (source.getOutputDataList().size() != 1 || target.getInputDataList().size() != 1) {
//            throw new Exception("source 或 target 具有多个输入输出，请指明要链接的Key Pair");
//        }
//        String sourceOutputKey = (String) source.getOutputDataList().keySet().toArray()[0];
//        String targetInputKey = (String) target.getInputDataList().keySet().toArray()[0];
//        this.keyPairs = new ArrayList<>();
//        this.keyPairs.add(new Pair<>(sourceOutputKey, targetInputKey));
//    }

    public Pair<String, String> getKeyPair() {
        assert this.keyPairs.size() > 0 : "未添加任何 keyPair";
        return this.keyPairs.get(0);
    }

    public List<Pair<String, String>> getKeyPairs() {
        return this.keyPairs;
    }

//    public Operator getTargetOperator() {
//        return this.targetOperator;
//    }
//
//    public Operator getSourceOperator() {
//        return this.sourceOperator;
//    }

}
