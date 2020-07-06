package fdu.daslab.executable.basic.utils;

import fdu.daslab.executable.basic.model.FunctionModel;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * 反射需要的工具类，如访问获取FunctionModel
 *
 * @author 唐志伟
 * @since 2020/7/6 1:39 PM
 * @version 1.0
 */
public class ReflectUtil {

    /**
     * 根据指定路径，加载class，构造对象，以及其中声明的方法
     * @param path class文件的路径
     * @return function实体
     */
    public static FunctionModel createInstanceAndMethodByPath(String path) {
        // 创建自定义classloader对象
        DiskClassLoader diskLoader = new DiskClassLoader(path);
        try {
            String[] paths = path.split("/");
            String className = paths[paths.length - 1].substring(0, paths[paths.length - 1].lastIndexOf(".class"));
            // 加载class文件
            Class<?> c = diskLoader.loadClass("fdu.daslab.executable.udf." + className);
            if (c != null) {
                try {
                    Object obj =  c.newInstance();
                    // 存在多个函数
                    Map<String, Method> functions = new HashMap<>();
                    for (Method method : c.getDeclaredMethods()) {
                        functions.put(method.getName(), method);
                    }
                    return new FunctionModel(obj, functions);
                } catch (InstantiationException | IllegalAccessException
                        | SecurityException | IllegalArgumentException e) {
                    e.printStackTrace();
                }
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }
}
