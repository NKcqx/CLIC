package fdu.daslab.executable.spark.operators.table;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import fdu.daslab.executable.spark.utils.SparkInitUtil;
import org.apache.spark.sql.*;

import java.util.List;
import java.util.Map;

/**
 * 从文件读取table的算子
 *
 * @author 刘丰艺
 * @since 2020/10/27 9:30 PM
 * @version 1.0
 */
public class TableSource extends OperatorBase<Dataset<Row>, Dataset<Row>> {
    public TableSource(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("SparkTableSource", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs,
                        ResultModel<Dataset<Row>> result) {
        SQLContext sqlContext = SparkInitUtil.getDefaultSQLContext();
        Dataset<Row> df = null;

        String tableName = "";
        String fileType = "";

        String inputPath = this.params.get("inputPath");
        if (inputPath == null) {
            throw new NullPointerException("inputPath cannot be null!");
        }
        if (inputPath.contains(".")) {
            tableName = inputPath.substring(inputPath.lastIndexOf("/") + 1, inputPath.lastIndexOf("."));
            fileType = inputPath.substring(inputPath.lastIndexOf("."), inputPath.length());
        } else {
            tableName = inputPath.substring(inputPath.lastIndexOf("/") + 1);
        }

        switch (fileType) {
            case ".txt":
                df = sqlContext.read().option("header", "true").csv(inputPath);
                break;
            case ".json":
                df = sqlContext.read().json(inputPath).toDF();
                break;
            default:
                // 默认以csv方式打开数据源文件
                // 如果源文件没有后缀，则按HDFS分布式存储来处理
                df = sqlContext.read().format("csv").option("header", "true").load(inputPath);
        }
        try {
            df.createTempView(tableName);
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
    }
}
