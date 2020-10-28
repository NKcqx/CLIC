package fdu.daslab.executable.sparksql.operators;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import fdu.daslab.executable.sparksql.utils.SparkInitUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Map;

/**
 * SparkSQL平台的sql语句执行算子
 *
 * @author 刘丰艺
 * @since 2020/10/27 9:30 PM
 * @version 1.0
 */
public class ExeOperator extends OperatorBase<Dataset<Row>, Dataset<Row>> {

    public ExeOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("SparkSQLExeOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs,
                        ResultModel<Dataset<Row>> result) {
        SparkSession sparkSession = SparkInitUtil.getDefaultSparkSession();
        Dataset<Row> exeResult = sparkSession.sql(this.params.get("sqlText"));
        this.setOutputData("result", exeResult);
    }
}
