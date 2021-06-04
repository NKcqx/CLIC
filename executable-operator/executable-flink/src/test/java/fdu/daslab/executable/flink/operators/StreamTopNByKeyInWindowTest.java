package fdu.daslab.executable.flink.operators;

import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.utils.ReflectUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

/**
 * Created on 2021/5/23.
 * 测试实时topN
 * @author 李姜辛
 */
public class StreamTopNByKeyInWindowTest {

    List<String> d1 = Arrays.asList("2019-12-25 00:03:00 UTC,1,001".split(","));
    List<String> d2 = Arrays.asList("2019-12-25 00:03:10 UTC,1,002".split(","));
    List<String> d3 = Arrays.asList("2019-12-25 00:03:30 UTC,1,003".split(","));
    List<String> d4 = Arrays.asList("2019-12-25 00:03:45 UTC,1,002".split(","));
    List<String> d5 = Arrays.asList("2019-12-25 00:03:48 UTC,1,003".split(","));
    List<String> d6 = Arrays.asList("2019-12-25 00:03:55 UTC,1,001".split(","));
    List<String> d7 = Arrays.asList("2019-12-25 00:03:56 UTC,1,002".split(","));
    List<String> d8 = Arrays.asList("2019-12-25 00:03:57 UTC,1,002".split(","));
    List<String> d9 = Arrays.asList("2019-12-25 00:03:58 UTC,1,003".split(","));
    List<String> d10 = Arrays.asList("2019-12-25 00:03:59 UTC,1,002".split(","));

    List<List<String>> mydata = Arrays.asList(d1,d2,d3,d4,d5,d6,d7,d8,d9,d10);
    private StreamExecutionEnvironment fsEnv;
    @Before
    public void setUp() throws Exception {
        fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        fsEnv.setParallelism(1);
    }

    @After
    public void tearDown() throws Exception {

    }

    /**
     * 测试timeStamp/ReduceInWindow/TopN Operator
     *
      */

    @Test
    public void topNTest() {

        final FunctionModel functionModel = ReflectUtil.createInstanceAndMethodByPath("D:/study/data/udf/TestHotItemFunc.class");
        ParamsModel inputArgs = new ParamsModel(functionModel);
        inputArgs.setFunctionClasspath("D:/study/data/udf/TestHotItemFunc.class");

        /* timeStamp Operator */
        DataStream<List<String>> listDataStream = fsEnv.fromCollection(mydata);
        Map<String, String> params = new HashMap<String, String>();
        List<String> in = new LinkedList<String>();
        in.add("data");
        List<String> out = new LinkedList<String>();
        out.add("result");
        params.put("udfName", "assignTimestampFunc");
        StreamAssignTimestampOperator timeStamp = new StreamAssignTimestampOperator("1", in, out, params);
        timeStamp.setInputData("data", listDataStream);
        timeStamp.execute(inputArgs, null);
        DataStream<List<String>> nextStream1 = timeStamp.getOutputData("result");

        /* reduceINWindow Operator*/
        params.put("udfName", "reduceFunc");
        params.put("keyName", "reduceKey");
        params.put("winFunc", "windowFunc");
        params.put("timeInterval", "1");
        params.put("timeStep", "1");
        StreamReduceByKeyInWindow streamReduceByKeyInWindow = new StreamReduceByKeyInWindow("2", in, out, params);
        streamReduceByKeyInWindow.setInputData("data", nextStream1);
        streamReduceByKeyInWindow.execute(inputArgs, null);
        DataStream<List<String>> nextStream2 = streamReduceByKeyInWindow.getOutputData("result");

        /* topN Operator */
        params.put("id", "topNID");
        params.put("keyName", "topNKey");
        params.put("windowEnd", "topNWindow");
        params.put("topSize", "3");
        StreamTopNByKeyInWindow streamTopNByKeyInWindow = new StreamTopNByKeyInWindow("3", in, out, params);
        streamTopNByKeyInWindow.setInputData("data", nextStream2);
        streamTopNByKeyInWindow.execute(inputArgs, null);
        DataStream<String> result = streamTopNByKeyInWindow.getOutputData("result");
        result.print();
        try {
            fsEnv.execute();
        }catch (Exception e){
            e.printStackTrace();
        }


    }
}