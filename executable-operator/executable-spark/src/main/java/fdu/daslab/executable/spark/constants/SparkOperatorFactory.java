package fdu.daslab.executable.spark.constants;

import fdu.daslab.executable.basic.model.OperatorFactory;
import fdu.daslab.executable.spark.operators.*;
import fdu.daslab.executable.spark.operators.table.*;

import java.util.HashMap;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/9/3 10:36 上午
 */
public class SparkOperatorFactory extends OperatorFactory {

    public SparkOperatorFactory() {
        operatorMap = new HashMap<String, Class>() {{
            put("SourceOperator", FileSource.class);
            put("SinkOperator", FileSink.class);
            put("FilterOperator", FilterOperator.class);
            put("MapOperator", MapOperator.class);
            put("JoinOperator", JoinOperator.class);
            put("ReduceByKeyOperator", ReduceByKeyOperator.class);
            put("SortOperator", SortOperator.class);
            put("SocketSourceOperator", SocketSource.class);
            put("SocketSinkOperator", SocketSink.class);
            put("CountByValueOperator", CountByValueOperator.class);
            put("CountOperator", CountOperator.class);
            put("DistinctOperator", DistinctOperator.class);
            put("LoopOperator", LoopOperator.class);
            put("NextIteration", NextIteration.class);
            put("CollectionSink", CollectionSink.class);
            put("CollectionSource", CollectionSource.class);
            put("TableSourceOperator", TableSource.class);
            put("QueryOperator", QueryOperator.class);
            put("TableSinkOperator", TableSink.class);
            put("ToTableOperator", ToTableOperator.class);
            put("FromTableOperator", FromTableOperator.class);
            put("TFilterOperator", TFilterOperator.class);
            put("TJoinOperator", TJoinOperator.class);
            put("TProjectOperator", TProjectOperator.class);
            put("TRelationOperator", TRelationOperator.class);
            put("TAggregateOperator", TAggregateOperator.class);
        }};
    }

}
