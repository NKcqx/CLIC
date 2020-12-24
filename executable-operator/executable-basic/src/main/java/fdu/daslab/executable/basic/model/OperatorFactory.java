package fdu.daslab.executable.basic.model;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/8/19 11:52 上午
 */
public abstract class OperatorFactory {

       protected Map<String, Class> operatorMap;

       // 构建operator
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
