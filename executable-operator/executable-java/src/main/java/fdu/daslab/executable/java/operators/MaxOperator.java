package fdu.daslab.executable.java.operators;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Java平台 max算子
 * @author Du Qinghua
 * @version 1.0
 * @since 2020/09/10 10:20
 */
@Parameters(separators = "=")
public class MaxOperator extends OperatorBase<Stream<List<String>>, Stream<List<String>>> {

    // 通过指定路径来获取代码的udf
    @Parameter(names = {"--udfName"})
    String sortFunctionName;

    public MaxOperator(String id, List<String> inputKeys, List<String> outputKeys,
                       Map<String, String> params) {
        super("MaxOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Stream<List<String>>> result) {
        FunctionModel compareFunction = inputArgs.getFunctionModel();
        assert compareFunction != null;
        List<String> maxValue = this.getInputData("data")
                .max((data1, data2) -> (int) compareFunction.invoke(this.params.get("udfName"), data1, data2))
                .orElse(null);

        this.setOutputData("result", Stream.of(maxValue));
    }
}
