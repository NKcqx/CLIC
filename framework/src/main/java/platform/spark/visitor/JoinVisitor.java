package platform.spark.visitor;

import basic.Param;
import basic.operators.Operator;
import channel.Channel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import platform.spark.SparkPlatform;

import java.util.List;
import java.util.Map;

public final class JoinVisitor implements SparkVisitor {
    private Operator leftChild;
    private Operator rightChild;
    private EqualJoinPredicatePair equalJoinPredicatePair;

    private JoinVisitor(Operator left, Operator right, Param param) {
        this.leftChild = left;
        this.rightChild = right;
        String predicate = param.getData();
        int leftEnd = predicate.indexOf('=');
        int leftStart = predicate.indexOf('$') + 1;
        int rightStart = predicate.indexOf('$', leftEnd) + 1;
        this.equalJoinPredicatePair = new EqualJoinPredicatePair(
                predicate.substring(leftStart, leftEnd), predicate.substring(rightStart));
    }

    public static JoinVisitor newInstance(Operator operator) {
        List<Channel> inputChannel = operator.getInputChannel();
        Map<String, Param> inputParameterList = operator.getInputDataList();
        Param inputParameter = inputParameterList.get("predicate");
        return new JoinVisitor(inputChannel.get(0).getSourceOperator(),
                inputChannel.get(1).getSourceOperator(), inputParameter);
    }

    @Override
    public Dataset<Row> execute(SparkSession sparkSession) {
        Dataset<Row> rightTable = (Dataset<Row>) SparkPlatform.convertOperator2SparkVisitor(this.rightChild).execute(sparkSession);
        Dataset<Row> leftTable = (Dataset<Row>) SparkPlatform.convertOperator2SparkVisitor(this.leftChild).execute(sparkSession);
        return leftTable.join(rightTable, leftTable.col(equalJoinPredicatePair.leftCol).
                equalTo(rightTable.col(equalJoinPredicatePair.rightCol)));
    }

    class EqualJoinPredicatePair {
        String leftCol;
        String rightCol;

        EqualJoinPredicatePair(String left, String right) {
            this.leftCol = left;
            this.rightCol = right;
        }
    }
}
