package fdu.daslab.executable.udf;

import java.io.Serializable;
import java.util.List;

/**
 * @author Du Qinghua
 * @version 1.0
 * @since 2020/09/10 14:09
 */
public class TestNewOperatorFunc implements Serializable {

    // max
    public int maxFunc(List<String> record1, List<String> record2) {
        //点击量示例  www.baidu.com  7           www.google.com    9
        return new Integer(record1.get(1)) - new Integer(record2.get(1));
    }
    // min
    public int minFunc(List<String> record1, List<String> record2) {
        //点击量示例  www.baidu.com  7      www.google.com    9
        return new Integer(record1.get(1)) - new Integer(record2.get(1));
    }
}
