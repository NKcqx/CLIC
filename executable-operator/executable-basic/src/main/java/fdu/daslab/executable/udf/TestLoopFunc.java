package fdu.daslab.executable.udf;

import java.util.List;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/9/25 7:38 下午
 */
public class TestLoopFunc {

    public boolean loopCondition(List<String> loopVar) {
        return Integer.parseInt(loopVar.get(0)) < 5;
    }

    public int increment(List<String> i) {
        String value = i.get(0);
        return Integer.parseInt(value) + 1;
    }

    public boolean loopBodyFilterFunc(List<String> list) {
        return true;
    }

    public List<String> loopBodyMapFunc(List<String> list) {
        for (int i = 0; i < list.size(); i++) {
            Integer realValue = Integer.parseInt(list.get(i));
            String incrementValue = String.valueOf(realValue + 1);
            list.set(i, incrementValue);
        }
        return list;
    }
}
