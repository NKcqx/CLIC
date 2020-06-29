package edu.daslab.executable.basic.model;

import edu.daslab.executable.basic.utils.ReflectUtil;

import java.io.Serializable;

/**
 * 参数模型
 */
public class ParamsModel<MODEL> implements Serializable {

    // 该算子的参数
    private final BasicOperator<MODEL> operatorParam;
    // 所有算子共同的参数，如函数参数
    private transient FunctionModel functionModel; // Method无法序列化

    // 对于spark等而言，无法序列化内部反射类，因此只记录class文件路径（目前需要将udf发送给各个workder端）
    private String functionClasspath;

    public void setFunctionClasspath(String functionClasspath) {
        this.functionClasspath = functionClasspath;
    }

    public ParamsModel(BasicOperator<MODEL> operatorParam, FunctionModel functionModel) {
        this.operatorParam = operatorParam;
        this.functionModel = functionModel;
    }

    public BasicOperator<MODEL> getOperatorParam() {
        return operatorParam;
    }

    public FunctionModel getFunctionModel() {
        // 针对无法序列化等情况下
        if (functionModel == null) {
            functionModel = ReflectUtil.createInstanceAndMethodByPath(functionClasspath);
        }
        return functionModel;
    }
}
