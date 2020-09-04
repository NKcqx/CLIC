package fdu.daslab.executable.java.constants;

import fdu.daslab.executable.basic.model.ExecutionOperator;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * java平台支持的所有算子
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/7/6 1:40 PM
 */
public final class JavaOperatorEnums {

    // 以下用于参数的传递
    public static final String FILE_SOURCE = "SourceOperator";  // 读取文件的source
    public static final String FILE_SINK = "SinkOperator";   // 写入文件的sink
    public static final String FILTER = "FilterOperator";
    public static final String MAP = "MapOperator";
    public static final String REDUCE_BY_KEY = "ReduceByKeyOperator";
    public static final String SORT = "SortOperator";
    public static final String JOIN = "JoinOperator";
//    public static final String TOP = "top";

    // 所有支持的operator
    public static Map<String, ExecutionOperator<Stream<List<String>>>> getAllOperators() {
        /*return new HashMap<String, ExecutionOperator<Stream<List<String>>>>() {{
            put(FILE_SOURCE, new FileSource());
            put(FILE_SINK, new FileSink());
            put(FILTER, new FilterOperator());
            put(MAP, new MapOperator());
            put(REDUCE_BY_KEY, new ReduceByKeyOperator());
            put(SORT, new SortOperator());
            //put(JOIN, new JoinOperator());
        }};*/
        return null;
    }

}
