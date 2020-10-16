package fdu.daslab.executable.spark.operators;

import com.beust.jcommander.Parameter;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;
import java.util.Map;

public class SparkHdfsSource extends OperatorBase<JavaRDD<List<String>>, JavaRDD<List<String>>> {

    // 输入路径
    @Parameter(names = {"--input"}, required = true)
    String inputFileName;

    // 输入的分隔符
    @Parameter(names = {"--separator"})
    String separateStr = ",";

    // 初始的partition数量
    @Parameter(names = {"--partitionNum"})
    Integer partitionNum = 5;

    public SparkHdfsSource(String name, String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("SparkHdfsSource", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<JavaRDD<List<String>>> result) {

    }
}
