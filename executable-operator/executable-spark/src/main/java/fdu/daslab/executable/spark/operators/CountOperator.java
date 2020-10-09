package fdu.daslab.executable.spark.operators;

import com.beust.jcommander.Parameters;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;
import java.util.Map;

/**
 * @author Du Qinghua
 * @version 1.0
 * @since 2020/09/18 15:49
 */
@Parameters(separators = "=")

public class CountOperator extends OperatorBase<JavaRDD<List<String>>, Long> {

    public CountOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("SparkCountOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Long> result) {
         final long countValue = this.getInputData("data")
                .count();
        this.setOutputData("result", countValue);
    }
}
