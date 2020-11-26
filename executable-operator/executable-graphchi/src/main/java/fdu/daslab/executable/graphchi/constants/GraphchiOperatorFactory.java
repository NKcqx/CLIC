package fdu.daslab.executable.graphchi.constants;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.OperatorFactory;
import fdu.daslab.executable.graphchi.operators.FileSinkOperator;
import fdu.daslab.executable.graphchi.operators.FileSourceOperator;
import fdu.daslab.executable.graphchi.operators.PageRankOperator;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Qinghua Du
 * @version 1.0
 * @since 2020/11/23 17:16
 */
public class GraphchiOperatorFactory implements OperatorFactory {
    private static Map<String, Class> operatorMap = new HashMap<String, Class>() {{
        put("PageRankOperator", PageRankOperator.class);
        put("SourceOperator", FileSourceOperator.class);
        put("SinkOperator", FileSinkOperator.class);

    }};

    @Override
    public OperatorBase createOperator(String name, String id, List<String> inputKeys,
                                       List<String> outputKeys, Map<String, String> params)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Class<?> optCls = operatorMap.get(name);
        // 构造函数的所有参数的参数类型
        Class[] type = {String.class, List.class, List.class, Map.class};
        Constructor constructor = optCls.getConstructor(type);
        return (OperatorBase) constructor.newInstance(id, inputKeys, outputKeys, params);

    }
}
