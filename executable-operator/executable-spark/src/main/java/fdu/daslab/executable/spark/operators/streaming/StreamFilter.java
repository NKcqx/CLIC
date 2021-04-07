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
 * streaming的filter算子
 *
 * @author lfy
 * @since 2021/4/6 3:30 PM
 * @version 1.0
 */
@Parameters(separators = "=")
public class StreamFilter extends OperatorBase<JavaDStream<List<String>>, JavaDStream<List<String>>> {

    public StreamFilter(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("StreamFilter", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<JavaDStream<List<String>>> result) {
        // 开始流处理
        JavaDStream<List<String>> inputStream = this.getInputData("data");

        String udfName = this.params.get("udfName");
        JavaDStream<List<String>> outputStream = inputStream.filter(data -> {
            FunctionModel functionModel = inputArgs.getFunctionModel();
            return (boolean) functionModel.invoke(udfName, data);
        });

        this.setOutputData("result", outputStream);
    }
}
