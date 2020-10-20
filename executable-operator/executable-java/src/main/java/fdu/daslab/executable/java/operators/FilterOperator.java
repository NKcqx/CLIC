package fdu.daslab.executable.java.operators;

import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * java平台的filter算子，可执行
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/7/6 1:47 PM
 */
@Parameters(separators = "=")
public class FilterOperator extends OperatorBase<Stream<List<String>>, Stream<List<String>>> {


    public FilterOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("JavaFilterOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs,
                        ResultModel<Stream<List<String>>> result) {
        FunctionModel functionModel = inputArgs.getFunctionModel();
        assert functionModel != null;
        Stream<List<String>> nextStream = this.getInputData("data")
                .filter(data -> (boolean) functionModel.invoke(this.params.get("udfName"), data));
        this.setOutputData("result", nextStream);

    }
}
