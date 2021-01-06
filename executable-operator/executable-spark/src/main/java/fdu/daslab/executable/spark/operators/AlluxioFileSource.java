package fdu.daslab.executable.spark.operators;

//import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import fdu.daslab.executable.spark.utils.SparkInitUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Spark平台的文件读取source
 *
 * @author 唐志伟
 * @since 2020/7/6 1:52 PM
 * @version 1.0
 */
@Parameters(separators = "=")
public class AlluxioFileSource extends OperatorBase<JavaRDD<List<String>>, JavaRDD<List<String>>> {

//    // 输入路径
//    @Parameter(names = {"--input"}, required = true)
//    String inputFileName;
//
//    // 输入的分隔符
//    @Parameter(names = {"--separator"})
//    String separateStr = ",";
//
//    // 初始的partition数量
//    @Parameter(names = {"--partitionNum"})
//    Integer partitionNum = 5;

    public AlluxioFileSource(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("SparkAlluxioFileSource", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs,
                        ResultModel<JavaRDD<List<String>>> result) {
        final JavaSparkContext javaSparkContext = SparkInitUtil.getDefaultSparkContext();
        // FileSource sourceArgs = (FileSource) inputArgs.getOperatorParam();
        // 读取文件，并按照分割符分隔开来


        //TODO: 将outputpath更改为alluxio的URI即可。
        //val s = sc.textFile("alluxio://localhost:19998/Input")
        final JavaRDD<List<String>> listJavaRDD = javaSparkContext
                .textFile(this.params.get("inputPath"), Integer.parseInt(this.params.get("partitionNum")))
                .map(line -> Arrays.asList(line.split(this.params.get("separator"))));
        // result.setInnerResult(listJavaRDD);
        this.setOutputData("result", listJavaRDD);
    }
}
