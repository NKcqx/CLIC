package platform.spark.visitor;

import basic.Operators.Operator;
import basic.Param;
import channel.Channel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

public class DataSourceVisitor implements SparkVisitor {
    private Map<String,Param> inputDataList;
    private String path;

    private DataSourceVisitor(String path){
        this.path=path;
    }
    @Override
    public Dataset<Row> execute(SparkSession sparkSession) {
//        String path=inputDataList.get("path").getData();
        return sparkSession.read().parquet(this.path);
    }

    public static DataSourceVisitor newInstance(Operator operator){
        Map<String, Param> inputParameter=operator.getInput_data_list();
        String path=inputParameter.get("data_path").getData(); //获取源文件路径
        return new DataSourceVisitor(path);
    }
}
