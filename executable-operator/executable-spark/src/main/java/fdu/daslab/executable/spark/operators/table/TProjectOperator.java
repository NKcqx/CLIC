package fdu.daslab.executable.spark.operators.table;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Map;

/**
 * table的select算子
 *
 * @author 刘丰艺
 * @since 2020/11/20 9:30 PM
 * @version 1.0
 */
public class TProjectOperator extends OperatorBase<Dataset<Row>, Dataset<Row>> {
    public TProjectOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("SparkTProjectOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Dataset<Row>> result) {
        Dataset<Row> df = this.getInputData("data");
        if (this.params.get("condition") == null) {
            throw new IllegalArgumentException("投影算子选择的列属性参数不能为空！");
        }
        if (!this.params.get("condition").equals("no need to exe")) {
            String[] cols = this.params.get("condition").split(",");
            if (cols.length == 1) {
                df = df.selectExpr(cols[0]);
            } else {
                df = df.selectExpr(cols);
            }
        }
        this.setOutputData("result", df);
    }
}
