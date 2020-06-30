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
 * Java平台的map函数
 */
@Parameters(separators = "=")
public class MapOperator implements BasicOperator<Stream<List<String>>> {

    // 通过指定路径来获取代码的udf
    @Parameter(names={"--udfName"})
    String mapFunctionName;

    @Override
    public void execute(ParamsModel<Stream<List<String>>> inputArgs,
                        ResultModel<Stream<List<String>>> result) {
        MapOperator mapArgs = (MapOperator) inputArgs.getOperatorParam();
        FunctionModel functionModel = inputArgs.getFunctionModel();
        assert functionModel != null;
        @SuppressWarnings("unchecked")
        Stream<List<String>> nextStream = result.getInnerResult()
                .map(data -> (List<String>) functionModel.invoke(mapArgs.mapFunctionName, data));
        result.setInnerResult(nextStream);
    }
}
