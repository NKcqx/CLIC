package fdu.daslab.executable.java.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * java平台的map函数，可执行
 *
 * @author 唐志伟，陈齐翔
 * @since 2020/7/6 1:47 PM
 * @version 1.0
 */
@Parameters(separators = "=")
public class MapOperator extends OperatorBase<Stream<List<String>>, Stream<List<String>>> {

    // 通过指定路径来获取代码的udf
    @Parameter(names = {"--udfName"})
    String mapFunctionName;

    public MapOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("MapOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs,
                        ResultModel<Stream<List<String>>> result) {
        // MapOperator mapArgs = (MapOperator) inputArgs.getOperatorParam();
        FunctionModel functionModel = inputArgs.getFunctionModel();
        assert functionModel != null;
        @SuppressWarnings("unchecked")
        Stream<List<String>> nextStream = this.getInputData("data")
                .map(data -> (List<String>) functionModel.invoke(this.params.get("udfName"), data));
        // @SuppressWarnings("unchecked")
        /*Stream<List<String>> nextStream = result.getInnerResult("data")
                .map(data -> (List<String>) functionModel.invoke(this.params.get("udfName"), data));*/

        // result.setInnerResult("result", nextStream);
        this.setOutputData("result", nextStream);
    }
}
