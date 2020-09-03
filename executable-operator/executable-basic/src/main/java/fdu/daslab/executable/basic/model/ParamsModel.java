package fdu.daslab.executable.basic.model;

import fdu.daslab.executable.basic.utils.ReflectUtil;

import java.io.Serializable;

/**
 * 平台的参数模型
 *
 * @param <MODEL> 每个平台定义流转的数据模型，比如Stream、RDD
 *
 * @author 唐志伟
 * @since 2020/7/6 1:35 PM
 * @version 1.0
 */
public class ParamsModel implements Serializable {

    // 该算子的参数
    // private final ExecutionOperator<MODEL> operatorParam;
    // 所有算子共同的参数，如函数参数
    private transient FunctionModel functionModel; // Method无法序列化

    // 对于spark等而言，无法序列化内部反射类，因此只记录class文件路径（目前需要将udf发送给各个workder端）
    private String functionClasspath;

    public void setFunctionClasspath(String functionClasspath) {
        this.functionClasspath = functionClasspath;
    }

    public ParamsModel(FunctionModel functionModel) {
        // this.operatorParam = operatorParam;
        this.functionModel = functionModel;
    }

    /*public ExecutionOperator<MODEL> getOperatorParam() {
        return operatorParam;
    }*/

    public FunctionModel getFunctionModel() {
        // 针对无法序列化等情况下
        if (functionModel == null) {
            functionModel = ReflectUtil.createInstanceAndMethodByPath(functionClasspath);
        }
        return functionModel;
    }
}
