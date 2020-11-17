package fdu.daslab.executable.spark.operators.table;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Map;

/**
 * 从数据库读取table
 *
 * @author 刘丰艺
 * @since 2020/11/16 2:00 PM
 * @version 1.0
 */
public class DBTableSource extends OperatorBase<DataFrameReader, Dataset<Row>> {
    public DBTableSource(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("SparkDBTableSource", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Dataset<Row>> result) {
        DataFrameReader reader = this.getInputData("data");
        String table = this.params.get("dbtable");
        reader.option("dbtable", table);
        Dataset<Row> df = reader.load();
        try {
            df.createTempView(table);
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
    }
}