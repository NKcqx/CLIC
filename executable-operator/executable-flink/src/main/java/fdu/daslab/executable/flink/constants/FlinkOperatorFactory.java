package fdu.daslab.executable.flink.constants;

import fdu.daslab.executable.basic.model.OperatorFactory;
import fdu.daslab.executable.flink.operators.*;

import java.util.HashMap;

/**
 * @author 李姜辛
 * @version 1.0
 * @since 2021/3/10 17:00
 */


public class FlinkOperatorFactory extends OperatorFactory {
    public FlinkOperatorFactory(){
        operatorMap = new HashMap<String, Class>() {{
            put("SourceOperator", FileSource.class);
            put("SinkOperator", FileSink.class);
            put("FilterOperator", FilterOperator.class);
            put("MapOperator", MapOperator.class);
//            put("FlatMapOperator"), FlatMapOperator.class);
//            put("JoinOperator", JoinOperator.class);
            put("ReduceByKeyOperator", ReduceByKeyOperator.class);
            put("SortOperator", SortOperator.class);
//            put("SocketSourceOperator", SocketSource.class);
//            put("SocketSinkOperator", SocketSink.class);
//            put("CountByValueOperator", CountByValueOperator.class);
            put("CountOperator", CountOperator.class);
            put("DistinctOperator", DistinctOperator.class);
//            put("LoopOperator", LoopOperator.class);
//            put("NextIteration", NextIteration.class);
//            put("CollectionSink", CollectionSink.class);
//            put("CollectionSource", CollectionSource.class);
            put("StreamSourceOperator", StreamFileSource.class);
            put("StreamSinkOperator", StreamFileSink.class);
            put("StreamFilterOperator", StreamFilterOperator.class);
            put("StreamMapOperator", StreamMapOperator.class);
            put("StreamReduceByKeyInWindow", StreamReduceByKeyInWindow.class);
            put("StreamAssignTimestampOperator", StreamAssignTimestampOperator.class);
            put("StreamTopNByKeyInWindow", StreamTopNByKeyInWindow.class);

//            put("TableSourceOperator", TableSource.class);
//            put("QueryOperator", QueryOperator.class);
//            put("TableSinkOperator", TableSink.class);
//            put("ToTableOperator", ToTableOperator.class);
//            put("FromTableOperator", FromTableOperator.class);
//            put("TFilterOperator", TFilterOperator.class);
//            put("TJoinOperator", TJoinOperator.class);
//            put("TProjectOperator", TProjectOperator.class);
//            put("TRelationOperator", TRelationOperator.class);
//            put("TAggregateOperator", TAggregateOperator.class);
        }};
    }
}
