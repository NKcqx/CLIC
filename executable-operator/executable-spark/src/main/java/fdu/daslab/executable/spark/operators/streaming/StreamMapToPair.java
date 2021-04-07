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
 * streaming的mapToPair算子
 *
 * @author lfy
 * @since 2021/4/6 3:30 PM
 * @version 1.0
 */
@Parameters(separators = "=")
public class StreamMapToPair extends OperatorBase<JavaDStream<List<String>>, JavaPairDStream<String, Integer>> {

    public StreamMapToPair(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("StreamMapToPair", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<JavaPairDStream<String, Integer>> result) {
        // 开始流处理
        JavaDStream<List<String>> inputStream = this.getInputData("data");

        String keyName = this.params.get("keyName");
        String valueName = this.params.get("valueName");
        JavaPairDStream<String, Integer> outputStream = inputStream.mapToPair(data -> {
            FunctionModel functionModel = inputArgs.getFunctionModel();
            String key = (String) functionModel.invoke(keyName, data);
            Integer value = Integer.valueOf((String) functionModel.invoke(valueName, data));
            return new Tuple2<>(key, value) ;
        });

        this.setOutputData("result", outputStream);
    }
}
