package platform.spark.visitor;

import basic.Operators.Operator;
import basic.Param;
import channel.Channel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import platform.spark.SparkPlatform;

import java.util.List;
import java.util.Map;

public class CollectVisitor implements SparkVisitor {
    private Operator child;

    public CollectVisitor(Operator child){
        this.child=child;
    }
    @Override
    public List<Row> execute(SparkSession sparkSession) {
        return ((Dataset<Row>)SparkPlatform.convertOperator2SparkVisitor(this.child).execute(sparkSession)).collectAsList();
    }

    public static CollectVisitor newInstance(Operator operator){
        List<Channel> channelList=operator.getInputChannel();
        Operator child=channelList.get(0).getSourceOperator();
//        Map<String, Param> inputParameter=operator.getInput_data_list();
        return new CollectVisitor(child);
    }
}