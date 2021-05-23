package fdu.daslab.executable.flink.operators;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

/**
 * Flink批数据写入文件的算子
 * @author 李姜辛
 * @version 1.0
 * @since 2021/3/14 15:52
 */
public class FileSink  extends OperatorBase<DataSet<List<String>>, DataSet<List<String>>> {

    Logger logger = LoggerFactory.getLogger(FileSource.class);

    public FileSink(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("FlinkFileSink", id, inputKeys, outputKeys, params);
    }


//    // 是否输出一个文件
//    @Parameter(names = {"--isCombined"})
//    Boolean isCombined = Boolean.TRUE;

    @Override
    public void execute(ParamsModel inputArgs,
                        ResultModel<DataSet<List<String>>> result) {
        // FileSink fileSinkArgs = (FileSink) inputArgs.getOperatorParam();
        // 写入文件

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String separator = this.params.get("separator");
        String outputPath = this.params.get("outputPath");

        final DataSet<String> res = this.getInputData("data")
                .map((MapFunction<List<String>, String>) line -> (String) StringUtils.join(line, separator));


        try {
            final List<String> collect = res.collect();
            Files.write(Paths.get(outputPath), collect);
        } catch (Exception e) {
            e.printStackTrace();
        }


        // TODO: 设置并行度

//        boolean isCombined = this.params.get("isCombined").equals("true"); // todo 之后会根据数据类型在外面自动转换
//        if (isCombined) {
//            // 为了方便其他的节点交互，提供将所有Partition写入一个文件的可能性
//            this.getInputData("data")
//                    .foreachPartition(partitionIter -> {
//                        // 所有数据均追加到一个文件上
//                        FileWriter fileWritter = new FileWriter(this.params.get("outputPath"), true);
//                        PrintWriter out = new PrintWriter(fileWritter);
//                        partitionIter.forEachRemaining(record -> {
//                            StringBuilder writeLine = new StringBuilder();
//                            record.forEach(field -> {
//                                writeLine.append(field);
//                                writeLine.append(this.params.get("separator"));
//                            });
//                            writeLine.deleteCharAt(writeLine.length() - 1);
//                            out.println(writeLine);
//                            out.flush();
//                        });
//                        out.close();
//                        fileWritter.close();
//                    });
//        } else {
//            // 一个partition写入一个文件
//            this.getInputData("data")
//                    .map(line -> StringUtils.join(line, this.params.get("separator")))
//                    .saveAsTextFile(this.params.get("outputPath"));
//        }
        try {
            // 数据准备好
            this.getMasterClient().postDataPrepared();
        } catch (TException e) {
            e.printStackTrace();
        }
    }

}
