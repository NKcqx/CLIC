package fdu.daslab.executable.spark.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.BasicOperator;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import fdu.daslab.executable.spark.utils.SparkInitUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Spark平台的文件读取source
 */
@Parameters(separators = "=")
public class FileSource implements BasicOperator<JavaRDD<List<String>>> {

    // 输入路径
    @Parameter(names = {"--input"}, required = true)
    String inputFileName;

    // 输入的分隔符
    @Parameter(names = {"--sep"})
    String separateStr = ",";

    // 初始的partition数量
    @Parameter(names = {"--partitionNum"})
    Integer partitionNum = 5;

    @Override
    public void execute(ParamsModel<JavaRDD<List<String>>> inputArgs,
                        ResultModel<JavaRDD<List<String>>> result) {
        final JavaSparkContext javaSparkContext = SparkInitUtil.getDefaultSparkContext();
        FileSource sourceArgs = (FileSource) inputArgs.getOperatorParam();
        // 读取文件，并按照分割符分隔开来
        final JavaRDD<List<String>> listJavaRDD = javaSparkContext
                .textFile(sourceArgs.inputFileName, sourceArgs.partitionNum)
                .map(line -> Arrays.asList(line.split(sourceArgs.separateStr)));
        result.setInnerResult(listJavaRDD);
    }
}
