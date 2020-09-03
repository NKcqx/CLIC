package fdu.daslab.executable.spark.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.*;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;
import java.util.Map;

/**
 * Spark平台的map函数
 *
 * @author 唐志伟
 * @since 2020/7/6 1:53 PM
 * @version 1.0
 */
@Parameters(separators = "=")
public class MapOperator extends OperatorBase<JavaRDD<List<String>>, JavaRDD<List<String>>> {
    // 通过指定路径来获取代码的udf
    @Parameter(names = {"--udfName"})
    String mapFunctionName;

    public MapOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("SparkMapOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<JavaRDD<List<String>>> result) {
        // MapOperator mapArgs = (MapOperator) inputArgs.getOperatorParam();
        @SuppressWarnings("unchecked") final JavaRDD<List<String>> nextStream = this.getInputData("data")
                .map(data -> {
                    // 因为无法序列化，只能传入可序列化的ParamsModel
                    FunctionModel functionModel = inputArgs.getFunctionModel();
                    return (List<String>) functionModel.invoke(this.params.get("udfName"), data);
                });
        this.setOutputData("result", nextStream);
    }
}
