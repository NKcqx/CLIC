package fdu.daslab.executable.spark.operators;

import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import fdu.daslab.executable.spark.utils.SparkInitUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.javatuples.Triplet;

import java.util.List;
import java.util.Map;

/**
 * 将JavaRDD<List<String>>转换成Dataset<Row>
 *
 * @author 刘丰艺
 * @since 2020/11/3 3:30 PM
 * @version 1.0
 */
public class ToTableOperator extends OperatorBase<JavaRDD<List<String>>, Dataset<Row>> {

    public ToTableOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("SparkToTableOperator", id, inputKeys, outputKeys, params);
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

        // 获取用户定义的schemaMap
        Map<String, Triplet<Class, Boolean, String>> schemaMap =
                (Map<String, Triplet<Class, Boolean, String>>) functionModel.invoke(this.params.get("udfName"));
        // 转成schema
        StructType tableSchema = getSchemaFromMap(schemaMap);

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

    /**
     * 将用户定义的schemaMap转成schema
     *
     * @param schemaMap
     * @return schema
     */
    private StructType getSchemaFromMap(Map<String, Triplet<Class, Boolean, String>> schemaMap) {
        int i = 0;
        StructField[] fields = new StructField[schemaMap.size()];
        for (Map.Entry<String, Triplet<Class, Boolean, String>> entry : schemaMap.entrySet()) {
            Class javaType = entry.getValue().getValue0();
            DataType sparkSQLType = getReflectType(javaType);
            fields[i] = new StructField(entry.getKey(), sparkSQLType, entry.getValue().getValue1(), Metadata.empty());
            i += 1;
        }
        return new StructType(fields);
    }

    /**
     * 将用户指定的类型映射成Spark SQL平台的数据类型
     * （这个函数或许单独写在一个工具类中？）
     *
     * @param javaType
     * @return
     */
    private DataType getReflectType(Class javaType) {
        DataType sparkSQLType = null;
        if (javaType.equals(Integer.class)) {
            sparkSQLType = DataTypes.IntegerType;
        }
        if (javaType.equals(String.class)) {
            sparkSQLType = DataTypes.StringType;
        }
        if (javaType.equals(Boolean.class)) {
            sparkSQLType = DataTypes.BooleanType;
        }
        if (javaType.equals(Float.class)) {
            sparkSQLType = DataTypes.FloatType;
        }
        if (javaType.equals(Double.class)) {
            sparkSQLType = DataTypes.DoubleType;
        }
        if (javaType.equals(Byte.class)) {
            sparkSQLType = DataTypes.ByteType;
        }
        if (javaType.equals(Long.class)) {
            sparkSQLType = DataTypes.LongType;
        }
        if (javaType.equals(Short.class)) {
            sparkSQLType = DataTypes.ShortType;
        }
        return sparkSQLType;
    }
}
