package edu.daslab.executable.spark.model;

import edu.daslab.executable.basic.model.ResultModel;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;

/**
 * 封装JavaRDD
 *
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
