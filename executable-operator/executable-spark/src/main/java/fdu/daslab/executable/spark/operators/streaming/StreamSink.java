package fdu.daslab.executable.spark.operators.streaming;

import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import fdu.daslab.executable.spark.utils.SparkInitUtil;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * streaming的sink算子
 *
 * @author lfy
 * @since 2021/4/6 3:30 PM
 * @version 1.0
 */
@Parameters(separators = "=")
public class StreamSink extends OperatorBase<JavaDStream<List<String>>, JavaDStream<List<String>>> {

    public StreamSink(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("StreamSink", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<JavaDStream<List<String>>> result) {
        // 获取streaming模式上下文
        final JavaStreamingContext streamingContext = SparkInitUtil.getDefaultStreamingContext();

        JavaDStream<List<String>> resultStream = this.getInputData("data");
        // 测试，打印输出
        resultStream.print();

        // 流处理在这一步才会触发执行
        streamingContext.start();
        try {
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
