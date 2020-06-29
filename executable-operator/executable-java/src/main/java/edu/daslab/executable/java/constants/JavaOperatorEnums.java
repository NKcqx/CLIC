package edu.daslab.executable.java.constants;

import edu.daslab.executable.basic.model.BasicOperator;
import edu.daslab.executable.java.operators.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;


/**
 * java平台支持的所有算子
 */
public class JavaOperatorEnums {

    // 以下用于参数的传递
    public static final String FILE_SOURCE = "file_source";  // 读取文件的source
    public static final String FILE_SINK = "file_sink";   // 写入文件的sink
    public static final String FILTER = "filter";
    public static final String MAP = "map";
    public static final String REDUCE_BY_KEY = "reduce_by_key";
    public static final String SORT = "sort";
    public static final String JOIN = "join";
//    public static final String TOP = "top";

    // 所有支持的operator
    public static Map<String, BasicOperator<Stream<List<String>>>> getAllOperators() {
        return new HashMap<String, BasicOperator<Stream<List<String>>>>() {{
            put(FILE_SOURCE, new FileSource());
            put(FILE_SINK, new FileSink());
            put(FILTER, new FilterOperator());
            put(MAP, new MapOperator());
            put(REDUCE_BY_KEY, new ReduceByKeyOperator());
            put(SORT, new SortOperator());
            put(JOIN, new JoinOperator());
        }};
    }

    private JavaOperatorEnums() {}
}
