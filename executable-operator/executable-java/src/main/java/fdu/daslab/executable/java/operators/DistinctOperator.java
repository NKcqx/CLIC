package fdu.daslab.executable.java.operators;


import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author Du Qinghua
 * @version 1.0
 * @since 2020/09/10 15:53
 */
@Parameters(separators = "=")
public class DistinctOperator extends OperatorBase<Stream<List<String>>, Stream<List<String>>> {

    public DistinctOperator(String id, List<String> inputKeys, List<String> outputKeys,
                       Map<String, String> params) {
        super("DistinctOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Stream<List<String>>> result) {

        Stream<List<String>> nextStream = this.getInputData("data").distinct();
        this.setOutputData("result", nextStream);
    }
}
