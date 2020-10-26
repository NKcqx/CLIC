package fdu.daslab.executable.sparksql.operators;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.OperatorFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparkSQLOperatorFactory implements OperatorFactory {

    private static Map<String, Class> opratorMap = new HashMap<String, Class>() {{
        put("SqlSourceOperator", FileSource.class);
        put("SqlExeOperator", ExeOperator.class);
        put("SqlSinkOperator", FileSink.class);
    }};

    @Override
    public OperatorBase createOperator(String name, String id, List<String> inputKeys,
                                       List<String> outputKeys, Map<String, String> params)
        throws NoSuchMethodException,
            IllegalAccessException, InvocationTargetException, InstantiationException {
        Class<?> optCls = opratorMap.get(name);
        // 构造函数的所有参数的参数类型
        Class[] type = {String.class, String.class, List.class, List.class, Map.class};
        Constructor constructor = optCls.getConstructor(type);
        return (OperatorBase) constructor.newInstance(name, id, inputKeys, outputKeys, params);
    }
}
