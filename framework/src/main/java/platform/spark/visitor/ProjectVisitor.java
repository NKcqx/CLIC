package platform.spark.visitor;

import basic.Operators.Operator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import platform.spark.SparkPlatform;

import java.util.Arrays;

public class ProjectVisitor implements SparkVisitor {
    private String[] colList;
    private Operator child;//table
    private ProjectVisitor(Operator table, String cols){
        colList=cols.split(",");
        this.child=table;
    }
    @Override
    public Dataset<Row> execute(SparkSession sparkSession) {
        Dataset<Row> table= (Dataset<Row>) SparkPlatform.convertOperator2SparkVisitor(this.child).execute(sparkSession);
        return table.select(colList[0],Arrays.copyOfRange(colList,1,colList.length));
    }

    public static ProjectVisitor newInstance(Operator operator){
        String cols=operator.getInput_data_list().get("predicate").getData();
        Operator srcTable=operator.getInputChannel().get(0).getSourceOperator();
        return new ProjectVisitor(srcTable,cols);
    }
}
