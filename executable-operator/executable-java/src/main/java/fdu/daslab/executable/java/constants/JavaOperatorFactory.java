package fdu.daslab.executable.java.constants;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.OperatorFactory;
import fdu.daslab.executable.java.operators.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/8/19 10:07 上午
 */
public class JavaOperatorFactory implements OperatorFactory {

    private static Map<String, Class> operatorMap = new HashMap<String, Class>() {{
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
    }};

    @Override
    public OperatorBase createOperator(String name, String id, List<String> inputKeys,
                                       List<String> outputKeys, Map<String, String> params)
            throws NoSuchMethodException,
            IllegalAccessException, InvocationTargetException, InstantiationException {
        Class<?> optCls = operatorMap.get(name);
        // 构造函数的所有参数的参数类型
        Class[] type = {String.class, List.class, List.class, Map.class};
        Constructor constructor = optCls.getConstructor(type);
        return (OperatorBase) constructor.newInstance(id, inputKeys, outputKeys, params);
    }
}
