package platform.spark.visitor;

import basic.operators.Operator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import platform.spark.SparkPlatform;

import java.util.Arrays;

public final class ProjectVisitor implements SparkVisitor {
    private String[] colList;
    private Operator child; //table

    private ProjectVisitor(Operator table, String cols) {
        colList = cols.split(",");
        this.child = table;
    }

    public static ProjectVisitor newInstance(Operator operator) {
        String cols = operator.getInputDataList().get("predicate").getData();
        Operator srcTable = operator.getInputChannel().get(0).getSourceOperator();
        return new ProjectVisitor(srcTable, cols);
    }

    @Override
    public Dataset<Row> execute(SparkSession sparkSession) {
        Dataset<Row> table = (Dataset<Row>) SparkPlatform.convertOperator2SparkVisitor(this.child).execute(sparkSession);
        return table.select(colList[0], Arrays.copyOfRange(colList, 1, colList.length));
    }
}
