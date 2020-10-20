package fdu.daslab.executable.spark.operators;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.OperatorFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/9/3 10:36 上午
 */
public class SparkOperatorFactory implements OperatorFactory {

    private static Map<String, Class> operatorMap = new HashMap<String, Class>() {{
        put("SourceOperator", FileSource.class);
        put("SinkOperator", FileSink.class);
        put("FilterOperator", FilterOperator.class);
        put("MapOperator", MapOperator.class);
        // put("JoinOperator", JoinOperator.class);
        put("ReduceByKeyOperator", ReduceByKeyOperator.class);
        put("SortOperator", SortOperator.class);
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