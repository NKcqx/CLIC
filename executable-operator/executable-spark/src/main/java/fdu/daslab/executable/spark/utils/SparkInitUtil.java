package fdu.daslab.executable.spark.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 初始化spark需要的一些方法
 *
 * @author 唐志伟
 * @since 2020/7/6 1:53 PM
 * @version 1.0
 */
public class SparkInitUtil {

    /**
     *  初始化JavaSparkContext
     *
     * @return JavaSparkContext
     */
    public static JavaSparkContext getDefaultSparkContext() {
        // 初始化spark
        SparkConf conf = new SparkConf()
                .setAppName("Test")
                .setMaster("local");
//        // 部分类需要序列化
//        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//        conf.set("spark.kryo.registrationRequired", "true");
//        conf.registerKryoClasses(new Class[]{
//                java.lang.reflect.Method.class,
//                edu.daslab.executable.basic.model.FunctionModel.class
//        });
        return new JavaSparkContext(conf);
    }
}
