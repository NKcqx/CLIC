package fdu.daslab.executable.sparksql.operators;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import fdu.daslab.executable.sparksql.utils.SparkInitUtil;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Map;

/**
 * SparkSQL平台的读取table算子
 *
 * @author 刘丰艺
 * @since 2020/10/27 9:30 PM
 * @version 1.0
 */
public class TableSource extends OperatorBase<Dataset<Row>, Dataset<Row>> {
    public TableSource(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("SparkSQLTableSource", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs,
                        ResultModel<Dataset<Row>> result) {
        SparkSession sparkSession = SparkInitUtil.getDefaultSparkSession();
        Dataset<Row> df = null;

        String inputPath = this.params.get("inputPath");
        String tableName = inputPath.substring(inputPath.lastIndexOf("/") + 1, inputPath.lastIndexOf("."));
        String fileType = inputPath.substring(inputPath.lastIndexOf("."), inputPath.length());

        switch (fileType) {
            case ".txt":
                df = sparkSession.read().option("header", "true").csv(inputPath);
                break;
            case ".json":
                df = sparkSession.read().json(inputPath).toDF();
                break;
            default:
                // 默认以csv方式打开数据源文件
                // 如果源文件没有后缀，则按HDFS分布式存储来处理
                df = sparkSession.read().format("csv").option("header", "true").load(inputPath);
        }
        try {
            df.createTempView(tableName);
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
        this.setOutputData("result", df);
    }
}
