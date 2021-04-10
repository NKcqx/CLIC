package fdu.daslab.executable.flink.constants;

import fdu.daslab.executable.basic.model.OperatorFactory;
import fdu.daslab.executable.flink.operators.*;

import java.util.HashMap;

/**
 * @author 李姜辛
 * @version 1.0
 * @since 2021/3/10 17:00
 */

// QUESTION(SOLVED): 在实现Spark的时候是根据什么来确定要实现哪些算子的？只看到了单元测试，除了单元测试以外，最终要能通过什么流处理的测试程序吗？
    // 实现主要的算子即可，最终需要写一个demo程序，调用系统的算子，参考framework/src/main/java/demo
public class FlinkOperatorFactory extends OperatorFactory {
    public FlinkOperatorFactory(){
        operatorMap = new HashMap<String, Class>() {{
            put("SourceOperator", FileSource.class);
            put("SinkOperator", FileSink.class);
            put("FilterOperator", FilterOperator.class);
            put("MapOperator", MapOperator.class);
//            put("FlatMapOperator"), FlatMapOperator.class); // QUESTION: 为什么Spark没有FlatMapOperator?
//            put("JoinOperator", JoinOperator.class); // QUESTION: 为什么这边Spark的JoinOperator报错？
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
