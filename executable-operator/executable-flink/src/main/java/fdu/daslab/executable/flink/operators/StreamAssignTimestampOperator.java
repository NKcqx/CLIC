package fdu.daslab.executable.flink.operators;

import com.beust.jcommander.Parameter;
import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import java.util.List;
import java.util.Map;

/**
 * flink流处理，给数据打上一个【事件】时间戳
 * @author 李姜辛
 * @version 1.0
 * @since 2021/3/30 15:25
 */
public class StreamAssignTimestampOperator extends OperatorBase<DataStream<List<String>>, DataStream<List<String>>> {
    // 通过指定路径来获取代码的udf
    @Parameter(names = {"--udfName"})
    String mapFunctionName;

    public StreamAssignTimestampOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("StreamFlinkAssignTimestamp", id, inputKeys, outputKeys, params);
    }


    @Override
    public void execute(ParamsModel inputArgs, ResultModel<DataStream<List<String>>> result) {
        // MapOperator mapArgs = (MapOperator) inputArgs.getOperatorParam();
//        final StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
//        fsEnv.setParallelism(1);

        // NOTE: 必须设置时间戳为事件时间戳，而不是系统时间戳，因为目前的流数据并不是真正的流数据，而是“带有时间戳的批数据”，从而模拟真正的流数据
        final StreamExecutionEnvironment fsEnv = this.getInputData("data").getExecutionEnvironment();
        fsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final String udfName = this.params.get("udfName");
        @SuppressWarnings("unchecked") final DataStream<List<String>> nextStream = this.getInputData("data")
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<List<String>>() {
                    @Override
                    public long extractAscendingTimestamp(List<String> element) {
                        FunctionModel functionModel = inputArgs.getFunctionModel();
                        return (long) functionModel.invoke(udfName, element);
                    }
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
