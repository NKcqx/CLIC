package fdu.daslab.executable.spark.model;

import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;

/**
 * 封装JavaRDD的ResultModel
 *
 * @author 唐志伟
 * @since 2020/7/6 1:52 PM
 * @version 1.0
 */
public class RddResult implements ResultModel<JavaRDD<List<String>>> {

    // 内部轮转的rdd
    private JavaRDD<List<String>> innerRDD;

    @Override
    public void setInnerResult(JavaRDD<List<String>> result) {
        this.innerRDD = result;
    }

    @Override
    public JavaRDD<List<String>> getInnerResult() {
        return innerRDD;
    }

}
