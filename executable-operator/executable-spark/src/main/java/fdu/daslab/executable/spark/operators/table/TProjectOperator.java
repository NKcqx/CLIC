package fdu.daslab.executable.spark.operators.table;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Map;

public class TProjectOperator extends OperatorBase<Dataset<Row>, Dataset<Row>> {
    public TProjectOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("SparkTProjectOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Dataset<Row>> result) {
        Dataset<Row> df = this.getInputData("data").select();
        this.setOutputData("result", df);
    }
}
