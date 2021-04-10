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

// QUESTION(SOLVED): 要把流批处理的算子都实现出来吗？先实现流处理算子？流处理算子的名称要和批处理算子区别开吗？要由用户决定调用哪种算子吗？
// ANS: 要；先实现批处理并跑通；要区别；是的，所以逻辑算子也要添加流处理算子
// QUESTION: 逻辑算子中添加流处理算子后，是否需要在framework/src/main/resources/operator里面新增一个配置文件夹？

// QUESTION(SOLVED): Spark读取文件时给的partition参数是什么含义？是并行度的意思吗？是不是类似setParallelism的方法？
// 是的，类似。实际上每个平台都有独特的参数，需要在framework/src/main/resources/operator里面的xml里设置，这些参数一般用户不会用到，提供给高级用户调参用
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
