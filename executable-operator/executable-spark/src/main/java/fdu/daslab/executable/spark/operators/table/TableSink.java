package fdu.daslab.executable.spark.operators.table;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Map;

/**
 * 将table写入文件的算子
 *
 * @author 刘丰艺
 * @since 2020/10/27 9:30 PM
 * @version 1.0
 */
public class TableSink extends OperatorBase<Dataset<Row>, Dataset<Row>> {

    public TableSink(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("SparkTableSink", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs,
                        ResultModel<Dataset<Row>> result) {
        this.getInputData("data")
                .coalesce(Integer.parseInt(this.params.get("partitionNum"))) // 文件分区数量
                .write()
                .mode("overwrite") // 保存方式为覆盖
                .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") // 保存文件时去除success文件
                .option("header", "true")
                .csv(this.params.get("outputPath"));
    }
}
