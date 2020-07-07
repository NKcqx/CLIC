package fdu.daslab.executable.java.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.BasicOperator;
import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;

import java.util.List;
import java.util.stream.Stream;

/**
 * java平台的filter算子，可执行
 *
 * @author 唐志伟
 * @since 2020/7/6 1:47 PM
 * @version 1.0
 */
@Parameters(separators = "=")
public class FilterOperator implements BasicOperator<Stream<List<String>>> {

    // 通过指定路径来获取代码的udf
    @Parameter(names = {"--udfName"})
    String filterFunctionName;

    @Override
    public void execute(ParamsModel<Stream<List<String>>> inputArgs,
                        ResultModel<Stream<List<String>>> result) {
        FilterOperator filterArgs = (FilterOperator) inputArgs.getOperatorParam();
        FunctionModel functionModel = inputArgs.getFunctionModel();
        assert functionModel != null;
        Stream<List<String>> nextStream = result.getInnerResult()
                .filter(data -> (boolean) functionModel.invoke(filterArgs.filterFunctionName, data));
        result.setInnerResult(nextStream);
    }
}
