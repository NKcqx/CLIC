package fdu.daslab.executable.flink.operators;

import com.beust.jcommander.Parameter;
import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;
import java.util.Map;

/**
 * Flink平台的流处理Filter算子
 * @author 李姜辛
 * @version 1.0
 * @since 2021/3/11 10:46
 */

public class StreamFilterOperator extends OperatorBase<DataStream<List<String>>, DataStream<List<String>>> {

    // 通过指定路径来获取代码的udf
    @Parameter(names = {"--udfName"})
    String filterFunctionName;

    public StreamFilterOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("StreamFlinkFilterOperator", id, inputKeys, outputKeys, params);
    }
    @Override
    public void execute(ParamsModel inputArgs, ResultModel<DataStream<List<String>>> result) {
        final String udfName = this.params.get("udfName");
        final DataStream<List<String>> nextStream = this.getInputData("data")
                .filter(data -> {
                    // 因为无法序列化，只能传入可序列化的ParamsModel
                    FunctionModel functionModel = inputArgs.getFunctionModel();
                    return (boolean) functionModel.invoke(udfName, data);
                });
        this.setOutputData("result", nextStream);
    }
}
