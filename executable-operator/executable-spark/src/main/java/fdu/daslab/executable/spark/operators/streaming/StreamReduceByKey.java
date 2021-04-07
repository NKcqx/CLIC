package fdu.daslab.executable.spark.operators.streaming;

import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * streaming的reduceByKey算子
 *
 * @author lfy
 * @since 2021/4/6 3:30 PM
 * @version 1.0
 */
@Parameters(separators = "=")
public class StreamReduceByKey extends OperatorBase<JavaPairDStream<String, Integer>, JavaPairDStream<String, Integer>> {

    public StreamReduceByKey(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("StreamReduceByKey", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<JavaPairDStream<String, Integer>> result) {
        // 开始流处理
        JavaPairDStream<String, Integer> inputStream = this.getInputData("data");

        JavaPairDStream<String, Integer> outputStream = inputStream.reduceByKey(
                (i1, i2) -> i1 + i2
        );

        this.setOutputData("result", outputStream);
    }
}
