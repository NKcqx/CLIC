package fdu.daslab.executable.spark.operators.table;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 将Dataset<Row>转换成JavaRDD<List<String>>
 *
 * @author 刘丰艺
 * @since 2020/11/3 3:30 PM
 * @version 1.0
 */
public class FromTableOperator extends OperatorBase<Dataset<Row>, JavaRDD<List<String>>> {

    public FromTableOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("SparkFromTableOperator", id, inputKeys, outputKeys, params);
    }

    /**
     * 类型转换步骤：
     * Dataset<Row> -> JavaRDD<Row> -> JavaRDD<List<String>>
     * @param inputArgs 参数列表
     * @param result 返回的结果
     */
    @Override
    public void execute(ParamsModel inputArgs, ResultModel<JavaRDD<List<String>>> result) {
        JavaRDD<Row> rowRDD = this.getInputData("data").toJavaRDD();
        JavaRDD<List<String>> convertResult = rowRDD.map(new Function<Row, List<String>>() {
            @Override
            public List<String> call(Row row) throws Exception {
                List<String> line = new ArrayList<>();
                String lineStr = row.toString();
                if (lineStr.length() == 0) {
                    return null;
                } else {
                    // row.toString()会给每行的头尾加中括号[]，需要将其删去
                    lineStr = lineStr.substring(1, lineStr.length() - 1);
                    String[] items = lineStr.split(",");
                    for (String item : items) {
                        line.add(item);
                    }
                    return line;
                }
            }
        });
        this.setOutputData("result", convertResult);
    }
}
