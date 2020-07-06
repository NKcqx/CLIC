package fdu.daslab.executable.basic.model;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * 定义的function实体，包含function的对象，以及其中声明的方法
 *
 * @author 唐志伟
 * @since 2020/7/6 1:22 PM
 * @version 1.0
 */
public class FunctionModel {

    private final Object obj;   //对象
    private final Map<String, Method> functionMap;

    public FunctionModel(Object obj, Map<String, Method> functionMap) {
        this.obj = obj;
        this.functionMap = functionMap;
    }


    /**
     * 运行其中的方法
     * @param args 参数列表
     * @return 结果
     */
    public Object invoke(String functionName, Object... args) {
        try {
            return functionMap.get(functionName).invoke(obj, args);
        } catch (IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;
    }
}
