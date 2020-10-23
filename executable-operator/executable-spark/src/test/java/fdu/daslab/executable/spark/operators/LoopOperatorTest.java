package fdu.daslab.executable.spark.operators;

import fdu.daslab.executable.basic.model.Connection;
import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/10/9 1:02 下午
 */
public class LoopOperatorTest {
    private LoopOperator loopOperator;
    private CollectionSink collectionSink;
    private SparkOperatorFactory sparkOperatorFactory = new SparkOperatorFactory();
    private JavaSparkContext javaSparkContext;

    @Before
    public void before() {
        SparkInitUtil.setSparkContext(
                new SparkConf().setMaster("local[*]").setAppName("LoopOperatorTest"));
        javaSparkContext = SparkInitUtil.getDefaultSparkContext();
        List<String> inputValue = Arrays.asList("1", "2", "3", "4", "5");
        List<List<String>> inputValueBox = new ArrayList<>();
        inputValueBox.add(inputValue);

        List<String> loopVar = Collections.singletonList("1");
        List<List<String>> loopVarBox = Collections.singletonList(loopVar);

        List<String> inputKeys = Arrays.asList("loopVar", "data");

        List<String> outputKeys = Arrays.asList("loopVar", "result");

        Map<String, String> params = new HashMap<>();
        params.put("predicateName", "loopCondition");
        params.put("loopBody", "      operators:\n" +
                "      - inputKeys:\n" +
                "        - data\n" +
                "        name: MapOperator\n" +
                "        id: MapOperator-677696474\n" +
                "        outputKeys:\n" +
                "        - result\n" +
                "        params:\n" +
                "          udfName: loopBodyMapFunc\n" +
                "      dag:\n" +
                "      - id: MapOperator-677696474");
        params.put("loopVarUpdateName", "increment");

        try {
            this.collectionSink = (CollectionSink) this.sparkOperatorFactory
                    .createOperator("CollectionSink", "2", Collections.singletonList("data"), Collections.singletonList("result"), new HashMap<>());
            this.loopOperator = (LoopOperator) this.sparkOperatorFactory.createOperator(
                    "LoopOperator", "0", inputKeys, outputKeys, params);

            this.loopOperator.setInputData("data", javaSparkContext.parallelize(inputValueBox));
            this.loopOperator.setInputData("loopVar", javaSparkContext.parallelize(loopVarBox));

            this.loopOperator.connectTo("result", collectionSink, "data");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testLoop() { // 检查 fileSink 的inputData
        try {
            final FunctionModel functionModel = ReflectUtil.createInstanceAndMethodByPath("/Users/jason/Desktop/TestLoopFunc.class");
            ParamsModel inputArgs = new ParamsModel(functionModel);
            inputArgs.setFunctionClasspath("/Users/jason/Desktop/TestLoopFunc.class");
            // 不能使用之前的BFSTraversal
            Queue<OperatorBase<JavaRDD<List<String>>, JavaRDD<List<String>>>> bfsQueue = new LinkedList<>();
            bfsQueue.add(this.loopOperator);
            while (!bfsQueue.isEmpty()) {
                OperatorBase<JavaRDD<List<String>>, JavaRDD<List<String>>> curOpt = bfsQueue.poll();
                curOpt.execute(inputArgs, null);

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

            List<String> expectedResult = Arrays.asList("5", "6", "7", "8", "9");
            List<String> result = collectionSink.getOutputData("result");
            Assert.assertFalse(result.isEmpty());
            Assert.assertEquals(expectedResult, result);
        } catch (Exception ignored) {
        }

    }

    @Test
    public void testConstructLoopThroughStr() {
        try {
            Map<String, String> params = new HashMap<>();
            params.put("predicateName", "loopCondition");
            params.put("loopBody", "      operators:\n" +
                    "      - inputKeys:\n" +
                    "        - data\n" +
                    "        name: MapOperator\n" +
                    "        id: MapOperator-677696474\n" +
                    "        outputKeys:\n" +
                    "        - result\n" +
                    "        params:\n" +
                    "          udfName: loopBodyMapFunc\n" +
                    "      dag:\n" +
                    "      - id: MapOperator-677696474");
            params.put("loopVarUpdateName", "increment");

            LoopOperator loopOperator = (LoopOperator) this.sparkOperatorFactory.createOperator(
                    "LoopOperator", "0",
                    Arrays.asList("loopVar", "data"),
                    Arrays.asList("loopVar", "result"),
                    params);

            Assert.assertFalse(loopOperator.getOutputConnections().isEmpty());
            OperatorBase targetOpt = loopOperator.getOutputConnections().get(0).getTargetOpt();
            Assert.assertTrue(targetOpt.getName().contains("MapOperator"));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testConstructLoopByHand() {
        try {
            OperatorBase mapOperator = new SparkOperatorFactory().createOperator(
                    "MapOperator",
                    "id",
                    Collections.singletonList("data"),
                    Collections.singletonList("result"),
                    new HashMap<String, String>(){{
                        put("udfName", "loopBodyMapFunc");
                    }}
            );

            OperatorBase mapOperator2 = new SparkOperatorFactory().createOperator(
                    "MapOperator",
                    "id",
                    Collections.singletonList("data"),
                    Collections.singletonList("result"),
                    new HashMap<String, String>(){{
                        put("udfName", "loopBodyMapFunc");
                    }}
            );

            mapOperator.connectTo("result", mapOperator2, "data");

            Map<String, String> params = new HashMap<>();
            params.put("predicateName", "loopCondition");
            LoopOperator loopOperator = (LoopOperator) this.sparkOperatorFactory.createOperator(
                    "LoopOperator", "0",
                    Arrays.asList("loopVar", "data"),
                    Arrays.asList("loopVar", "result"),
                    params);

            // 此时还没设置连接到loopBody的边，不应该能拿到下一跳，无论是trigger 还是 real 的
            Assert.assertTrue(loopOperator.getOutputConnections().isEmpty());
            loopOperator.startLoopBody(mapOperator);

            // 此时应该能拿到body的起点，即loop的triggerConnection，不应为空
            Assert.assertFalse(loopOperator.getOutputConnections().isEmpty());
            Object mapOpt = loopOperator.getOutputConnections().get(0).getTargetOpt();
            // 下一跳应该是Map
            Assert.assertTrue(mapOpt instanceof MapOperator);
            Assert.assertEquals(mapOpt, mapOperator);

            // 但此时还没有endBody呢，而loop内部需要令 loopEnd 连接到loop自己创建的nextIteration
            // 所以在没endBody的时候，loopEnd的下一跳应该是空的
            Assert.assertTrue(mapOperator2.getOutputConnections().isEmpty());
            loopOperator.endLoopBody(mapOperator2);
            // 此时应该能拿到end的下一跳（nextIteration）
            Assert.assertFalse(mapOperator2.getOutputConnections().isEmpty());
            Object nextIteration = mapOperator2.getOutputConnections().get(0);
            Assert.assertTrue(nextIteration instanceof NextIteration);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @After
    public void after() {
        SparkInitUtil.getDefaultSparkContext().close();
    }
}
