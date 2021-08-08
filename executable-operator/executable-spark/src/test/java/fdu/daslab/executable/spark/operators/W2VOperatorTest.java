package fdu.daslab.executable.spark.operators;

import fdu.daslab.executable.basic.model.*;
import fdu.daslab.executable.basic.utils.ReflectUtil;
import fdu.daslab.executable.spark.constants.SparkOperatorFactory;
import fdu.daslab.executable.spark.utils.SparkInitUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.javatuples.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2021/8/8 下午4:02
 */
public class W2VOperatorTest {
    private JavaSparkContext javaSparkContext;
    private FileSource source = null;
    private W2VOperator w2v = null;
    private FileSink sink = null;
    private SparkOperatorFactory sparkOperatorFactory = new SparkOperatorFactory();
    @Before
    public void before() {
        HashMap<String, String> params = new HashMap<>();
        params.put("inputPath", "/Users/jason/Desktop/Spark-w2v-senti/ptb-sample.txt");
        params.put("separator", " ");
        try{
            this.source = (FileSource) sparkOperatorFactory.createOperator(
                    "SourceOperator",
                    "1",
                    Collections.singletonList("data"),
                    Collections.singletonList("result"),
                    params);
            HashMap<String, String> params2 = new HashMap<>();
            params2.put("vectorSize", "16");
            this.w2v = (W2VOperator) sparkOperatorFactory.createOperator(
              "W2VOperator",
              "2",
                    Collections.singletonList("data"),
                    Collections.singletonList("result"),
                    params2
            );

            HashMap<String, String> params3 = new HashMap<>();
            params3.put("outputPath", "/Users/jason/Desktop/Spark-w2v-senti/output/w2v_output");
            params3.put("separator", "$");
            this.sink  = (FileSink) sparkOperatorFactory.createOperator(
              "SinkOperator",
              "3",
              Collections.singletonList("data"),
              Collections.singletonList("result"),
              params3
            );
            this.source.connectTo("result", this.w2v, "data");
            this.w2v.connectTo("result", this.sink, "data");
        }catch (Exception e){
            e.printStackTrace();
        }

        SparkInitUtil.setSparkContext(
                new SparkConf().setMaster("local[*]").setAppName("W2VOperatorTest"));
        javaSparkContext = SparkInitUtil.getDefaultSparkContext();

    }

    @Test
    public void w2v() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        try {
            // 不能使用之前的BFSTraversal
            Queue<OperatorBase<JavaRDD<List<String>>, JavaRDD<List<String>>>> bfsQueue = new LinkedList<>();
            bfsQueue.add(this.source);
            while (!bfsQueue.isEmpty()) {
                OperatorBase<JavaRDD<List<String>>, JavaRDD<List<String>>> curOpt = bfsQueue.poll();
                curOpt.execute(null, null);

                List<Connection> connections = curOpt.getOutputConnections(); // curOpt没法明确泛化类型
                for (Connection connection : connections) {
                    OperatorBase<JavaRDD<List<String>>, JavaRDD<List<String>>> targetOpt = connection.getTargetOpt();
                    bfsQueue.add(targetOpt);

                    List<Pair<String, String>> keyPairs = connection.getKeys();
                    for (Pair<String, String> keyPair : keyPairs) {
                        JavaRDD<List<String>> sourceResult = curOpt.getOutputData(keyPair.getValue0());
                        // 将当前opt的输出结果传入下一跳的输入数据
                        targetOpt.setInputData(keyPair.getValue1(), sourceResult);
                    }
                }
            }
            List<List<String>> result = this.w2v.getOutputData("result").collect() ;
            Assert.assertFalse(result.isEmpty());
            String vec = result.get(0).get(0);
            String[] realRes = vec.substring(1, vec.length()-1).split(",");
            Assert.assertEquals(realRes.length, 10);
        } catch (Exception ignored) {
        }
    }

    @After
    public void after() {
        SparkInitUtil.getDefaultSparkContext().close();
    }
}
