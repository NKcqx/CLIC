package fdu.daslab.executable.spark.constants;

import fdu.daslab.executable.basic.model.ExecutionOperator;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;
import java.util.Map;


/**
 * spark平台支持的算子的枚举
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/7/6 1:51 PM
 */
public final class SparkOperatorEnums {

    // 以下用于参数的传递
    public static final String FILE_SOURCE = "SourceOperator";  // 读取文件的source
    public static final String FILE_SINK = "SinkOperator";   // 写入文件的sink
    public static final String FILTER = "FilterOperator";
    public static final String MAP = "MapOperator";
    public static final String REDUCE_BY_KEY = "ReduceByKeyOperator";
    public static final String SORT = "SortOperator";
////    public static final String JOIN = "join";

    private SparkOperatorEnums() {
    }

    // 所有支持的operator
    public static Map<String, ExecutionOperator<JavaRDD<List<String>>>> getAllOperators() {
        return null;
//        return new HashMap<String, ExecutionOperator<JavaRDD<List<String>>>>() {{
//            put(FILE_SOURCE, new FileSource());
//            put(FILE_SINK, new FileSink());
//            put(FILTER, new FilterOperator());
//            put(MAP, new MapOperator());
//            put(REDUCE_BY_KEY, new ReduceByKeyOperator());
//            put(SORT, new SortOperator());
//        }};
    }
}
