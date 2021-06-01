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
 * @since 2020/09/10 16:36
 */
@Parameters(separators = "=")
public class CountOperator extends OperatorBase<Stream<List<String>>, Stream<Long>> {

    public CountOperator(String id, List<String> inputKeys, List<String> outputKeys,
                         Map<String, String> params) {
        super("CountOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Stream<Long>> result) {

        Long nextStream = this.getInputData("data").count();

        this.setOutputData("result", Stream.of(nextStream));
    }
}
