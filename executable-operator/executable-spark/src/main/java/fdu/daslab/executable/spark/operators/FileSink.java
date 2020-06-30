package fdu.daslab.executable.spark.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.BasicOperator;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;

/**
 * 写入文件
 */
@Parameters(separators = "=")
public class FileSink implements BasicOperator<JavaRDD<List<String>>> {
    // 输入路径
    @Parameter(names={"--output"}, required = true)
    String outputFileName;

    // 输出的分隔符
    @Parameter(names={"--sep"})
    String separateStr = ",";

    @Override
    public void execute(ParamsModel<JavaRDD<List<String>>> inputArgs,
                        ResultModel<JavaRDD<List<String>>> result) {
        FileSink fileSinkArgs = (FileSink) inputArgs.getOperatorParam();
        // 写入文件
        result.getInnerResult()
                .map(line -> StringUtils.join(line, fileSinkArgs.separateStr))
                .saveAsTextFile(fileSinkArgs.outputFileName);
    }
}
