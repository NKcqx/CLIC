package fdu.daslab.executable.spark.operators;

import com.beust.jcommander.Parameter;
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
 * @since 2020/09/18 19:27
 */
@Parameters(separators = "=")

public class DistinctOperator extends OperatorBase<JavaRDD<List<String>>, JavaRDD<List<String>>> {
    @Parameter(names = {"--partitionNum"})
    Integer partitionNum = 5;

    public DistinctOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("SparkCountOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<JavaRDD<List<String>>> result) {
        final JavaRDD<List<String>> countValue = this.getInputData("data")
                .distinct(partitionNum);
        this.setOutputData("result", countValue);
    }
}

