package fdu.daslab.executable.sparksql.constants;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.OperatorFactory;
import fdu.daslab.executable.sparksql.operators.ExeOperator;
import fdu.daslab.executable.sparksql.operators.FileSink;
import fdu.daslab.executable.sparksql.operators.FileSource;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/10/27 10:30 pm
 */
public class SparkSQLOperatorFactory implements OperatorFactory {

    private static Map<String, Class> operatorMap = new HashMap<String, Class>() {{
        put("SqlSourceOperator", FileSource.class);
        put("SqlExeOperator", ExeOperator.class);
        put("SqlSinkOperator", FileSink.class);
    }};

    @Override
    public OperatorBase createOperator(String name, String id, List<String> inputKeys,
                                       List<String> outputKeys, Map<String, String> params)
            throws NoSuchMethodException,
            IllegalAccessException, InvocationTargetException, InstantiationException {
        if (!operatorMap.containsKey(name)) {
            throw new NoSuchElementException();
        }
        Class<?> optCls = operatorMap.get(name);
        // 构造函数的所有参数的参数类型
        Class[] type = {String.class, List.class, List.class, Map.class};
        Constructor constructor = optCls.getConstructor(type);
        return (OperatorBase) constructor.newInstance(id, inputKeys, outputKeys, params);
    }
}
