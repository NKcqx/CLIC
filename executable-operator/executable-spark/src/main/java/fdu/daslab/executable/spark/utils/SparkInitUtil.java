package fdu.daslab.executable.spark.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 初始化spark需要的一些方法
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/7/6 1:53 PM
 */
public class SparkInitUtil {

    // SparkContext，这里的配置实际上没有意义，配置通过参数传递
    private static JavaSparkContext sparkContext = null;


    /**
     * 初始化JavaSparkContext
     *
     * @return JavaSparkContext
     */
    public static JavaSparkContext getDefaultSparkContext() {
        // 初始化spark
//        // 部分类需要序列化
//        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//        conf.set("spark.kryo.registrationRequired", "true");
//        conf.registerKryoClasses(new Class[]{
//                java.lang.reflect.Method.class,
//                edu.daslab.executable.basic.model.FunctionModel.class
//        });
        if (sparkContext == null) {
            sparkContext = new JavaSparkContext(new SparkConf());
        }
        return sparkContext;
    }


    public static boolean setSparkContext(SparkConf conf) {
        if (SparkInitUtil.sparkContext == null) {
            SparkInitUtil.sparkContext = new JavaSparkContext(conf);
            return true;
        } else {
            return false;
        }
    }
}
