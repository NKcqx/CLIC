package fdu.daslab.executable.java.constants;

import fdu.daslab.executable.basic.model.OperatorFactory;
import fdu.daslab.executable.java.operators.*;

import java.util.HashMap;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/8/19 10:07 上午
 */
public class JavaOperatorFactory extends OperatorFactory {

    // 初始化所有的operator的映射关系
    public JavaOperatorFactory() {
        operatorMap = new HashMap<String, Class>() {{
            put("AlluxioSourceOperator", AlluxioFileSource.class);
            put("AlluxioSinkOperator", AlluxioFileSink.class);
            put("SourceOperator", FileSource.class);
            put("SinkOperator", FileSink.class);
            put("FilterOperator", FilterOperator.class);
            put("MapOperator", MapOperator.class);
            put("JoinOperator", JoinOperator.class);
            put("ReduceByKeyOperator", ReduceByKeyOperator.class);
            put("SortOperator", SortOperator.class);
            put("SocketSourceOperator", SocketSource.class);
            put("SocketSinkOperator", SocketSink.class);
            put("ParquetFileToRowSourceOperator", ParquetFileToRowSource.class);
            put("ParquetFileFromRowSinkOperator", ParquetFileFromRowSink.class);
            put("ParquetFileToColumnSourceOperator", ParquetFileToColumnSource.class);
            put("ParquetFileFromColumnSinkOperator", ParquetFileFromColumnSink.class);
            put("CountOperator", CountOperator.class);
            put("DistinctOperator", DistinctOperator.class);
            put("MaxOperator", MaxOperator.class);
            put("MinOperator", MinOperator.class);
            put("LoopOperator", LoopOperator.class);
            put("NextIteration", NextIteration.class);
            put("CollectionSource", CollectionSource.class);
            put("CollectionSink", CollectionSink.class);
            put("TableSourceOperator", TableSource.class);
        }};
    }
}
