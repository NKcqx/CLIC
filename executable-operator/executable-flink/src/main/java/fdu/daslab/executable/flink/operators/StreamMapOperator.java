package fdu.daslab.executable.flink.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;
import java.util.Map;

/**
 * Flink平台的流处理map算子
 * @author 李姜辛
 * @version 1.0
 * @since 2021/3/11 11:03
 */

@Parameters(separators = "=")

public class StreamMapOperator extends OperatorBase<DataStream<List<String>>, DataStream<List<String>>> {

    // 通过指定路径来获取代码的udf
    @Parameter(names = {"--udfName"})
    String mapFunctionName;

    public StreamMapOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("StreamFlinkMapOperator", id, inputKeys, outputKeys, params);
    }


    @Override
    public void execute(ParamsModel inputArgs, ResultModel<DataStream<List<String>>> result) {
        // MapOperator mapArgs = (MapOperator) inputArgs.getOperatorParam();
//        final StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
//        fsEnv.setParallelism(1);
        final String udfName = this.params.get("udfName");
        @SuppressWarnings("unchecked") final DataStream<List<String>> nextStream = this.getInputData("data")
                .map((MapFunction<List<String>, List<String>>) data -> {
                    // 因为无法序列化，只能传入可序列化的ParamsModel
                    FunctionModel functionModel = inputArgs.getFunctionModel();
                    return (List<String>) functionModel.invoke(udfName, data);
                })
                .returns(TypeInformation.of(new TypeHint<List<String>>(){}))
                ;
//        nextStream.print();
//        final StreamExecutionEnvironment fsEnv = nextStream.getExecutionEnvironment();
//        try {
//            fsEnv.execute();
//        } catch (Exception e){
//            e.printStackTrace();
//        }
        this.setOutputData("result", nextStream);
    }
}
