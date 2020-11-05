package fdu.daslab.executable.spark.operators;

import fdu.daslab.executable.basic.model.FunctionModel;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import fdu.daslab.executable.basic.utils.ReflectUtil;
import fdu.daslab.executable.spark.utils.SparkInitUtil;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

/**
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/10/8 4:30 PM
 */
public class QueryOperatorTest {

    private QueryOperator queryOperator;

    private SparkSession sparkSession;

    @Before
    public void before() throws AnalysisException {
        sparkSession = SparkInitUtil.getDefaultSparkSession();

        StructField[] studentFields = new StructField[] {
          new StructField("id", DataTypes.StringType, true, Metadata.empty()),
          new StructField("name", DataTypes.StringType, true, Metadata.empty())
        };
        StructField[] gradeFields = new StructField[] {
                new StructField("id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("cid", DataTypes.StringType, true, Metadata.empty()),
                new StructField("grade", DataTypes.StringType, true, Metadata.empty())
        };
        StructField[] courseFields = new StructField[] {
                new StructField("cid", DataTypes.StringType, true, Metadata.empty()),
                new StructField("cname", DataTypes.StringType, true, Metadata.empty())
        };

        StructType studentType = new StructType(studentFields);
        StructType gradeType = new StructType(gradeFields);
        StructType courseType = new StructType(courseFields);

        List<Row> stuRows = new ArrayList<>();
        stuRows.add(RowFactory.create("1", "Ming Xiao"));
        stuRows.add(RowFactory.create("2", "Hua Li"));
        stuRows.add(RowFactory.create("3", "Jianguo Li"));
        List<Row> graRows = new ArrayList<>();
        graRows.add(RowFactory.create("2", "A209", "80"));
        graRows.add(RowFactory.create("3", "B102", "95"));
        graRows.add(RowFactory.create("5", "B102", "75"));
        List<Row> courRows = new ArrayList<>();
        courRows.add(RowFactory.create("A209", "Math"));
        courRows.add(RowFactory.create("B102", "History"));
        courRows.add(RowFactory.create("B205", "Data Science"));

        Dataset<Row> stuDF = sparkSession.createDataFrame(stuRows, studentType);
        stuDF.createTempView("student");
        Dataset<Row> graDF = sparkSession.createDataFrame(graRows, gradeType);
        graDF.createTempView("grade");
        Dataset<Row> courDF = sparkSession.createDataFrame(courRows, courseType);
        courDF.createTempView("course");
    }

    @Test
    public void testExeOperator() {
        String id = "1788933234";
        List<String> inputKeys = Arrays.asList("data");
        List<String> outputKeys = Arrays.asList("result");
        Map<String, String> params = new HashMap<>();
        params.put("sqlText", "SELECT student.id,grade,cname "
                + "FROM (student JOIN grade on student.id=grade.id AND grade>80) JOIN course ON grade.cid=course.cid");
        queryOperator = new QueryOperator(id, inputKeys, outputKeys, params);

        // 下面几个参数没有实际作用，只是为了满足旧版的execute函数
        final FunctionModel functionModel = ReflectUtil
                .createInstanceAndMethodByPath("/Users/jason/Desktop/TestLoopFunc.class");
        ParamsModel inputArgs = new ParamsModel(functionModel);
        ResultModel<Dataset<Row>> fakeResult = null;

        // 执行算子
        queryOperator.execute(inputArgs, fakeResult);
        Dataset<Row> resultDF = queryOperator.getOutputData("result");

        // assert阶段
        List<List<Object>> resultRows = new ArrayList<>();
        List<List<Object>> expectedRows = new ArrayList<>();
        // fieldNames获取列属性名
        String[] fieldNames = resultDF.schema().fieldNames();
        // rows获取所有行
        Row[] rows = (Row[]) resultDF.collect();
        // 遍历每一行
        for (Row row : rows) {
            List<Object> values = new ArrayList<>();
            for (int i = 0; i < fieldNames.length; i++) {
                Object obj = row.get(i);
                values.add(obj);
            }
            resultRows.add(values);
        }
        //expectedRows.add(Arrays.asList("2", 80, "Math"));
        expectedRows.add(Arrays.asList("3", "95", "History"));
        Assert.assertEquals(expectedRows, resultRows);
    }
}
