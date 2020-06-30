package fdu.daslab.executable.udf;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * 测试word count的函数
 */
public class TestWordCountsFunc implements Serializable {

    // filter函数
    public boolean filterFunc(List<String> record) {
        List<String> needFiltered = Arrays.asList("u1", "u2", "u3");
        return needFiltered.contains(record.get(1));
    }

    // map函数
    public List<String> mapFunc(List<String> record) {
        return Arrays.asList(record.get(0), "1");
    }

    // reduce的key
    public String reduceKey(List<String> record) {
        return record.get(0);
    }

    // reduce的func
    public List<String> reduceFunc(List<String> record1, List<String> record2) {
        return Arrays.asList(record1.get(0),
                String.valueOf(new Integer(record1.get(1)) + new Integer(record2.get(1))));
    }

    // sort
    public int sortFunc(List<String> record1, List<String> record2) {
        return new Integer(record2.get(1)) - new Integer(record1.get(1));
    }
}
