package fdu.daslab.executable.flink.operators;

import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * Flink流处理，先keyBy, 再根据时间窗口分配数据，然后对于每个窗口中的相同key做reduce操作
 * @author 李姜辛
 * @version 1.0
 * @since 2021/3/11 11:33
 */

@Parameters(separators = "=")
public class StreamReduceByKeyInWindow extends OperatorBase<DataStream<List<String>>, DataStream<List<String>>> {

    //    // 通过指定路径来获取代码的udf
//    @Parameter(names = {"--udfName"})
//    String reduceFunctionName;
//
//    // 获取key的function
//    @Parameter(names = {"--keyName"})
//    String keyExtractFunctionName;

    public StreamReduceByKeyInWindow(String id,
                                     List<String> inputKeys,
                                     List<String> outputKeys,
                                     Map<String, String> params) {
        super("StreamFlinkReduceByKeyedWindow", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs,
                        ResultModel<DataStream<List<String>>> result) {
        // ReduceByKeyOperator reduceArgs = (ReduceByKeyOperator) inputArgs.getOperatorParam();
        final String keyName = this.params.get("keyName");
        final String udfName = this.params.get("udfName");
        final String winFunc = this.params.get("winFunc");
        final DataStream<List<String>> nextStream = this.getInputData("data")
                .keyBy((KeySelector<List<String>, String>) data -> {
                    // 因为无法序列化，只能传入可序列化的ParamsModel
                    FunctionModel functionModel = inputArgs.getFunctionModel();
                    return (String) functionModel.invoke(keyName, data);
                })
                // 根据时间窗口分配数据
                // todo: 后续把时间作为用户定义参数传递
                .timeWindow(Time.minutes(8), Time.minutes(4))
                // 对于每个窗口中的相同key做reduce操作
                .reduce((ReduceFunction<List<String>>) (record1, record2) -> {
                    // 因为无法序列化，只能传入可序列化的ParamsModel
                    FunctionModel functionModel = inputArgs.getFunctionModel();
                    return (List<String>)
                            functionModel.invoke(udfName, record1, record2);
                }, new WindowFunction<List<String>, List<String>, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<List<String>> input, Collector<List<String>> out) throws Exception {
                        FunctionModel functionModel = inputArgs.getFunctionModel();
                        functionModel.invoke(winFunc, key, window, input, out);
                    }
                });

        this.setOutputData("result", nextStream);
    }
}
