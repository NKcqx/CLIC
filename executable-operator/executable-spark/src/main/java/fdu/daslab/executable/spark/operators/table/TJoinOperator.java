package fdu.daslab.executable.spark.operators.table;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Map;

public class TJoinOperator extends OperatorBase<Dataset<Row>, Dataset<Row>> {
    public TJoinOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("SparkTJoinOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Dataset<Row>> result) {
        Dataset<Row> leftDF = this.getInputData("leftTable");
        Dataset<Row> rightDF = this.getInputData("rightTable");
        leftDF = leftDF.join(rightDF, this.params.get("joinKey"));
        this.setOutputData("result", leftDF);
    }
}
