package fdu.daslab.executable.spark.operators.table;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import fdu.daslab.executable.spark.utils.SparkInitUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Map;

/**
 * table的filter算子
 *
 * @author 刘丰艺
 * @since 2020/11/20 9:30 PM
 * @version 1.0
 */
public class TFilterOperator extends OperatorBase<Dataset<Row>, Dataset<Row>> {
    public TFilterOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("SparkTFilterOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Dataset<Row>> result) {
        Dataset<Row> df = this.getInputData("data").filter(this.params.get("condition"));
        this.setOutputData("result", df);
    }
}
