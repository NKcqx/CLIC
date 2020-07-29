package platform.spark.visitor;

import basic.operators.Operator;
import basic.Param;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

public final class DataSourceVisitor implements SparkVisitor {
    private Map<String, Param> inputDataList;
    private String path;

    private DataSourceVisitor(String path) {
        this.path = path;
    }

    public static DataSourceVisitor newInstance(Operator operator) {
        Map<String, Param> inputParameter = operator.getInputParamList();
        String path = inputParameter.get("data_path").getData(); //获取源文件路径
        return new DataSourceVisitor(path);
    }

    @Override
    public Dataset<Row> execute(SparkSession sparkSession) {
//        String path=inputDataList.get("path").getData();
        return sparkSession.read().parquet(this.path);
    }
}
