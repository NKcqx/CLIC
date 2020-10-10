package operators;

import fdu.daslab.executable.basic.model.Connection;
import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.utils.ReflectUtil;
import fdu.daslab.executable.java.constants.JavaOperatorFactory;
import fdu.daslab.executable.java.operators.FileSink;
import fdu.daslab.executable.java.operators.LoopOperator;
import fdu.daslab.executable.java.operators.MapOperator;
import org.javatuples.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Stream;

import static org.junit.Assert.*;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/9/25 3:41 下午
 */
public class LoopOperatorTest {
    private LoopOperator loopOperator;
    private MapOperator loopBody;
    private FileSink fileSink;
    private JavaOperatorFactory javaOperatorFactory = new JavaOperatorFactory();

    @Before
    public void before() {
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

        List<String> sinkInputKeys = Collections.singletonList("data");
        Map<String, String> sinkParams = new HashMap<>();
        sinkParams.put("separator", " ");
        sinkParams.put("outputPath", "/tmp/clic_output/loopTest.txt");

        List<String> bodyInputKeys = Collections.singletonList("data");
        List<String> bodyOutputKeys = Collections.singletonList("result");
        Map<String, String> bodyParams = new HashMap<>();
        bodyParams.put("udfName", "loopBodyMapFunc");

        try {
            this.fileSink = (FileSink) this.javaOperatorFactory
                    .createOperator("SinkOperator", "2", sinkInputKeys, Collections.emptyList(), sinkParams);
            this.loopBody = (MapOperator) this.javaOperatorFactory
                    .createOperator("MapOperator", "1", bodyInputKeys, bodyOutputKeys, bodyParams);
            this.loopOperator = (LoopOperator) this.javaOperatorFactory.createOperator(
                    "LoopOperator", "0", inputKeys, outputKeys, params);


            this.loopOperator.setInputData("data", inputValueBox.stream());
            this.loopOperator.setInputData("loopVar", loopVarBox.stream());

            this.loopOperator.connectTo("result", fileSink, "data");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testLoop(){ // 检查 fileSink 的inputData
        try {
            final FunctionModel functionModel = ReflectUtil.createInstanceAndMethodByPath("TestLoopFunc.class");
            ParamsModel inputArgs = new ParamsModel(functionModel);
            // 不能使用之前的BFSTraversal
            Queue<OperatorBase<Stream<List<String>>, Stream<List<String>>>> bfsQueue = new LinkedList<>();
            bfsQueue.add(this.loopOperator);
            while (!bfsQueue.isEmpty()) {
                OperatorBase<Stream<List<String>>, Stream<List<String>>>  curOpt = bfsQueue.poll();
                curOpt.execute(inputArgs, null);

                List<Connection> connections = curOpt.getOutputConnections(); // curOpt没法明确泛化类型
                for (Connection connection : connections) {
                    OperatorBase<Stream<List<String>>, Stream<List<String>>> targetOpt = connection.getTargetOpt();
                    bfsQueue.add(targetOpt);

                    List<Pair<String, String>> keyPairs = connection.getKeys();
                    for (Pair<String, String> keyPair : keyPairs){
                        Stream<List<String>> sourceResult = curOpt.getOutputData(keyPair.getValue0());
                        // 将当前opt的输出结果传入下一跳的输入数据
                        targetOpt.setInputData(keyPair.getValue1(), sourceResult);
                    }
                }
            }

            File file = new File("/tmp/clic_output/loopTest.txt");
            assertTrue(file.exists());
            assertTrue(file.isFile());
            FileInputStream inputStream = new FileInputStream(file);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String line = bufferedReader.readLine();
            assertEquals(line, "5 6 7 8 9");
        }catch (Exception ignored){
        }

    }
}
