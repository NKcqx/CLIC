package fdu.daslab.executable.spark.operators;

import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import fdu.daslab.executable.spark.utils.SparkInitUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;

/**
 * 将JavaRDD<List<String>>转换成Dataset<Row>
 *
 * @author 刘丰艺
 * @since 2020/11/3 3:30 PM
 * @version 1.0
 */
public class RDDToTableOperator extends OperatorBase<JavaRDD<List<String>>, Dataset<Row>> {

    public RDDToTableOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("SparkRDDToTableOperator", id, inputKeys, outputKeys, params);
    }

    /**
     * 类型转换步骤：
     * JavaRDD<List<String>> -> JavaRDD<Row> -> Dataset<Row>
     * @param inputArgs 参数列表
     * @param result 返回的结果
     */
    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Dataset<Row>> result) {
        SparkSession sparkSession = SparkInitUtil.getDefaultSparkSession();
        FunctionModel functionModel = inputArgs.getFunctionModel();

        StructType tableSchema = (StructType) functionModel.invoke(this.params.get("udfName"));
        JavaRDD<Row> rowRDD = this.getInputData("data").map(new Function<List<String>, Row>() {
            @Override
            public Row call(List<String> line) throws Exception {
                String[] split = line.toArray(new String[line.size()]);
                return RowFactory.create(split);
            }
        });
        Dataset<Row> convertResult = sparkSession.createDataFrame(rowRDD, tableSchema);
        try {
            convertResult.createTempView(this.params.get("tableName"));
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
        this.setOutputData("result", convertResult);
    }
}
