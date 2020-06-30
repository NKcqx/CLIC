package fdu.daslab.executable.spark.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.BasicOperator;
import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;

/**
 * Spark平台的map函数
 */
@Parameters(separators = "=")
public class MapOperator implements BasicOperator<JavaRDD<List<String>>> {
    // 通过指定路径来获取代码的udf
    @Parameter(names={"--udfName"})
    String mapFunctionName;

    @Override
    public void execute(ParamsModel<JavaRDD<List<String>>> inputArgs, ResultModel<JavaRDD<List<String>>> result) {
        MapOperator mapArgs = (MapOperator) inputArgs.getOperatorParam();
        @SuppressWarnings("unchecked")
        final JavaRDD<List<String>> nextStream = result.getInnerResult()
                .map(data -> {
                    // 因为无法序列化，只能传入可序列化的ParamsModel
                    FunctionModel functionModel = inputArgs.getFunctionModel();
                    return (List<String>)functionModel.invoke(mapArgs.mapFunctionName, data);
                });
        result.setInnerResult(nextStream);
    }
}
