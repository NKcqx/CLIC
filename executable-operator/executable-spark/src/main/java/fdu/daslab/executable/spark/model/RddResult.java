package fdu.daslab.executable.spark.model;

import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.spark.api.java.JavaRDD;

import java.util.HashMap;
import java.util.Map;

/**
 * 封装JavaRDD的ResultModel
 *
 * @author 唐志伟
 * @since 2020/7/6 1:52 PM
 * @version 1.0
 */
public class RddResult<Type> implements ResultModel<JavaRDD<Type>> {

    // 内部轮转的rdd
    private Map<String, JavaRDD<Type>> innerRddMap = new HashMap<>();

    @Override
    public void setInnerResult(String key, JavaRDD<Type> result) {
        this.innerRddMap.put(key, result);
    }

    @Override
    public JavaRDD<Type> getInnerResult(String key) {
        return innerRddMap.get(key);
    }
}
