package fdu.daslab.executable.spark.operators;

import fdu.daslab.executable.spark.constants.SparkOperatorFactory;
import fdu.daslab.executable.spark.utils.SparkInitUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/10/10 6:17 下午
 */
public class CollectionSinkTest {
    private JavaSparkContext javaSparkContext;

    @Before
    public void before(){
        SparkInitUtil.setSparkContext(new SparkConf().setMaster("local[*]").setAppName("CountByValueOperatorTest"));
        javaSparkContext = SparkInitUtil.getDefaultSparkContext();
    }

    @Test
    public void collectionSource() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {

        CollectionSink collectionSink = (CollectionSink) new SparkOperatorFactory().createOperator(
                "CollectionSink", "1", Collections.singletonList("data"), Collections.singletonList("result"), new HashMap<>());
        List<String> input = Arrays.asList("1", "2", "3");
        JavaRDD<List<String>> inputData = javaSparkContext.parallelize(Collections.singletonList(input));
        collectionSink.setInputData("data", inputData);
        collectionSink.execute(null, null);
        List<String> output = collectionSink.getOutputData("result");
        Assert.assertEquals(output, input);
    }

    @After
    public void after(){
        SparkInitUtil.getDefaultSparkContext().close();
    }
}
