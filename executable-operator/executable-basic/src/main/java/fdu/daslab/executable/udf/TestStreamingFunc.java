package fdu.daslab.executable.udf;

import java.util.List;

/**
 * 测试spark streaming的udf
 *
 * @author lfy
 * @since 2021/4/6 3:30 PM
 * @version 1.0
 */
public class TestStreamingFunc {

    /**
     * 过滤，将行为是view的记录留下来
     */
    public boolean filterFunc(List<String> data) {
        return data.get(1).equals("view");
    }

    /**
     * 选定聚合的key
     */
    public String keyFunc(List<String> data) {
        return data.get(2);
    }

    /**
     * 选定聚合的value
     */
    public String valueFunc(List<String> data) {
        return "1";
    }
}
