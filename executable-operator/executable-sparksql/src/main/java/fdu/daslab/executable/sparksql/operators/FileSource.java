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
 * SparkSQL平台的读取数据源算子
 *
 * @author 刘丰艺
 * @since 2020/10/27 9:30 PM
 * @version 1.0
 */
public class FileSource extends OperatorBase<Dataset<Row>, Dataset<Row>> {
    public FileSource(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("SparkSQLFileSource", id, inputKeys, outputKeys, params);
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
            case ".csv":
                df = sparkSession.read().format("csv").option("header", "true").load(inputPath);
                break;
            case ".txt":
                df = sparkSession.read().option("header", "true").csv(inputPath);
            case ".json":
                df = sparkSession.read().json(inputPath).toDF();
                break;
            default:
                throw new IllegalStateException("Unexpected file type: " + fileType);
        }
        try {
            df.createTempView(tableName);
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
        this.setOutputData("result", df);
    }
}
