package fdu.daslab.executable.java.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Java平台sort算子
 *
 * @author 唐志伟，陈齐翔
 * @since 2020/7/6 1:48 PM
 * @version 1.0
 */
@Parameters(separators = "=")
public class SortOperator extends OperatorBase<Stream<List<String>>, Stream<List<String>>> {

    // 通过指定路径来获取代码的udf
    @Parameter(names = {"--udfName"})
    String sortFunctionName;

    public SortOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("SortOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs,
                        ResultModel<Stream<List<String>>> result) {
        // SortOperator sortArgs = (SortOperator) inputArgs.getOperatorParam();
        FunctionModel sortFunction = inputArgs.getFunctionModel();
        assert sortFunction != null;
        /*Stream<List<String>> nextStream = result.getInnerResult("data")
                .sorted((data1, data2) -> (int) sortFunction.invoke(this.params.get("udfName"), data1, data2));*/
        Stream<List<String>> nextStream = this.getInputData("data")
                .sorted((data1, data2) -> (int) sortFunction.invoke(this.params.get("udfName"), data1, data2));
        // result.setInnerResult("result", nextStream);
        this.setOutputData("result", nextStream);
    }
}
