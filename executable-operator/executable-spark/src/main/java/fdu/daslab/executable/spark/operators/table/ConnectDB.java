package fdu.daslab.executable.spark.operators.table;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import fdu.daslab.executable.spark.utils.SparkInitUtil;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SQLContext;

import java.util.List;
import java.util.Map;

/**
 * jdbc连接数据库
 *
 * @author 刘丰艺
 * @since 2020/11/16 2:00 PM
 * @version 1.0
 */
public class ConnectDB extends OperatorBase<DataFrameReader, DataFrameReader> {
    public ConnectDB(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("SparkConnectDB", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<DataFrameReader> result) {
        SQLContext sqlContext = SparkInitUtil.getDefaultSQLContext();
        DataFrameReader reader = sqlContext.read().format("jdbc");
        reader.option("url", this.params.get("url"));
        reader.option("driver", this.params.get("driver"));
        reader.option("user", this.params.get("user"));
        reader.option("password", this.params.get("password"));
        this.setOutputData("result", reader);
    }
}
