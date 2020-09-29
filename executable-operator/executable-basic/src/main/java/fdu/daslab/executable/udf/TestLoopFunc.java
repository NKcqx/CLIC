package fdu.daslab.executable.udf;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/9/25 7:38 下午
 */
public class TestLoopFunc {

    public boolean loopCondition(List<String> loopVar){
        return Integer.parseInt(loopVar.get(0)) < 5;
    }

    public int increment(List<String> i){
        String value = i.get(0);
        return Integer.parseInt(value) + 1;
    }

    public List<String> loopBodyMapFunc(List<String> list){
        list = list.stream().map(s -> String.valueOf(Integer.parseInt(s) + 1)).collect(Collectors.toList());
        return list;
    }
}
