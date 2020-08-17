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
 * Spark平台的filter算子，可执行
 *
 * @author 唐志伟
 * @since 2020/7/6 1:53 PM
 * @version 1.0
 */
@Parameters(separators = "=")
public class FilterOperator implements BasicOperator<JavaRDD<List<String>>> {

    // 通过指定路径来获取代码的udf
    @Parameter(names = {"--udfName"})
    String filterFunctionName;

    @Override
    public void execute(ParamsModel<JavaRDD<List<String>>> inputArgs,
                        ResultModel<JavaRDD<List<String>>> result) {
        FilterOperator filterArgs = (FilterOperator) inputArgs.getOperatorParam();

        final JavaRDD<List<String>> nextStream = result.getInnerResult()
                .filter(data -> {
                    // 因为无法序列化，只能传入可序列化的ParamsModel
                    FunctionModel functionModel = inputArgs.getFunctionModel();
                    return (boolean) functionModel.invoke(filterArgs.filterFunctionName, data);
                });
        result.setInnerResult(nextStream);
    }
}
