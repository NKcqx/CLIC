package fdu.daslab.executable.basic.model;

import org.javatuples.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 作用和Channel大致一样，但功能更少，只是为了用Key链接两个Opt
 *
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/8/19 5:30 下午
 */
public class Connection implements Serializable {
    private OperatorBase sourceOpt;
    private List<String> sourceKeys; // todo 这有一个弱相关性，即要求 sourceKey的顺序和targetKey的顺序是一致的!
    private OperatorBase targetOpt;
    private List<String> targetKeys;

    public Connection(OperatorBase sourceOpt, String sourceKey, OperatorBase targetOpt, String targetKey) {
        this.sourceOpt = sourceOpt;
        this.sourceKeys = new ArrayList<>();
        this.sourceKeys.add(sourceKey);

        this.targetOpt = targetOpt;
        this.targetKeys = new ArrayList<>();
        this.targetKeys.add(targetKey);
    }

    public void addKey(String sourceKey, String targetKey){
        sourceKeys.add(sourceKey) ;
        targetKeys.add(targetKey);
    }

    public List<Pair<String , String >> getKeys(){
        List<Pair<String , String >> res = new ArrayList<>();
        for (int i=0;i<sourceKeys.size(); i++){
            res.add( new Pair<>(sourceKeys.get(i), targetKeys.get(i)) );
        }
        return res;
    }

    public OperatorBase getSourceOpt() {
        return sourceOpt;
    }

    public List<String> getSourceKeys() {
        return sourceKeys;
    }

    public OperatorBase getTargetOpt() {
        return targetOpt;
    }

    public List<String> getTargetKeys() {
        return targetKeys;
    }
}
