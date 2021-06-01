package fdu.daslab.executable.spark.operators.table;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import fdu.daslab.executable.spark.utils.SparkInitUtil;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Map;

/**
 * 从文件读取table的算子（用于自动生成DAG的情况）
 *
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/11/20 9:30 PM
 */
public class TRelationOperator extends OperatorBase<Dataset<Row>, Dataset<Row>> {
    public TRelationOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("SparkTRelationOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Dataset<Row>> result) {
        SparkSession sparkSession = SparkInitUtil.getDefaultSparkSession();
        Dataset<Row> df = null;
        String tableName = "";
        String fileType = "";

        String inputPath = this.params.get("inputPath");
        if (inputPath.contains(".")) {
            tableName = inputPath.substring(inputPath.lastIndexOf("/") + 1, inputPath.lastIndexOf("."));
            fileType = inputPath.substring(inputPath.lastIndexOf("."), inputPath.length());
        } else {
            tableName = inputPath.substring(inputPath.lastIndexOf("/") + 1);
        }

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
