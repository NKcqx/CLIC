package platform.spark.visitor;

import basic.operators.Operator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import platform.spark.SparkPlatform;

public class FilterVisitor implements SparkVisitor {
    private Operator child;
    private String predicate;

    public FilterVisitor(Operator child, String predicate) {
        this.child = child;
        this.predicate = predicate;
    }

    public static FilterVisitor newInstance(Operator operator) {
        String predicate = operator.getInputDataList().get("predicate").getData();
        Operator srcOperator = operator.getInputChannel().get(0).getSourceOperator();
        return new FilterVisitor(srcOperator, predicate);
    }

    @Override
    public Dataset<Row> execute(SparkSession sparkSession) {
        Dataset<Row> src = (Dataset<Row>) SparkPlatform.convertOperator2SparkVisitor(this.child).execute(sparkSession);

        return src.filter(this.predicate);
    }
}
