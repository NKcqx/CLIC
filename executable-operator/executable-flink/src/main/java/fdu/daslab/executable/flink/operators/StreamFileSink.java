package fdu.daslab.executable.flink.operators;


import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;

/**
 * 流数据写入文件的算子
 * @author 李姜辛
 * @version 1.0
 * @since 2021/3/10 20:06
 */


public class StreamFileSink extends OperatorBase<DataStream<String>, DataStream<List<String>>> {
    public StreamFileSink(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("StreamFlinkFileSink", id, inputKeys, outputKeys, params);
    }


//    // 是否输出一个文件
//    @Parameter(names = {"--isCombined"})
//    Boolean isCombined = Boolean.TRUE;

    @Override
    public void execute(ParamsModel inputArgs,
                        ResultModel<DataStream<List<String>>> result) {
        // FileSink fileSinkArgs = (FileSink) inputArgs.getOperatorParam();
        // 写入文件
//        final StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        String separator = this.params.get("separator");
        String outputPath = this.params.get("outputPath");

        final DataStream<String> dataStream = this.getInputData("data");

        dataStream.print();
        final StreamExecutionEnvironment fsEnv = dataStream.getExecutionEnvironment();

        try {
            fsEnv.execute();
        } catch (Exception e){
            e.printStackTrace();
        }

//        try {
//            // 数据准备好
//            this.getMasterClient().postDataPrepared();
//        } catch (TException e) {
//            e.printStackTrace();
//        }
    }
}
