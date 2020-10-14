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
 * @since 2020/09/18 16:04
 */
@Parameters(separators = "=")

public class CountByValueOperator extends OperatorBase<JavaRDD<List<String>>, Map<List<String>, Long>> {

    public CountByValueOperator(String id, List<String> inputKeys, List<String> outputKeys,
                                Map<String, String> params) {
        super("SparkCountByValueOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Map<List<String>, Long>> result) {
        final Map<List<String>, Long> countValue = this.getInputData("data")
                .countByValue();
        this.setOutputData("result", countValue);
    }
}
