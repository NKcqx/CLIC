package fdu.daslab.executable.flink.operators;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Flink平台读入source到流数据
 * @author 李姜辛
 * @version 1.0
 * @since 2021/3/10 17:03
 */


public class StreamFileSource extends OperatorBase<DataStream<List<String>>, DataStream<List<String>>> {

    public StreamFileSource(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("StreamFlinkFileSource", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs,
                        ResultModel<DataStream<List<String>>> result) {

        // flink stream env
        final StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        fsEnv.setParallelism(1);

        final String separator = this.params.get("separator");
        // FileSource sourceArgs = (FileSource) inputArgs.getOperatorParam();
        // 读取文件，并按照分割符分隔开来
        final DataStream<List<String>> listDataStream = fsEnv
                .readTextFile(this.params.get("inputPath"))  // Flink 读取文件没有 minPartition 这个参数,
                                                             // 只有将元素划分给下游算子的rescaling
                                                             // spark的partition是否相当于flink的parallelism?
                .map(line -> Arrays.asList(line.split(separator)))
                .returns(TypeInformation.of(new TypeHint<List<String>>(){}))
                ;
        // result.setInnerResult(listJavaRDD);
//        listDataStream.print();
//        try {
//            fsEnv.execute();
//        } catch (Exception e){
//            e.printStackTrace();
//        }
        this.setOutputData("result", listDataStream);

        // 是否需要env.execute()？
    }
}
