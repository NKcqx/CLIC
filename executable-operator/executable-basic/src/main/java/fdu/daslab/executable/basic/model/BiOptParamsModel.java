package fdu.daslab.executable.basic.model;

import fdu.daslab.executable.basic.utils.ReflectUtil;

import java.io.Serializable;

/**
 * @author 唐志伟，刘丰艺
 * @version 1.0
 * @since 2020/7/6 14:05
 */
public class BiOptParamsModel<MODEL> implements Serializable {

    // 该算子参数
    private final BinaryExecutionOperator<MODEL> operatorParam;
    // 所有算子共同的参数
    private transient FunctionModel functionModel;

    // 对于spark等而言，无法序列化内部反射类，因此只记录class文件路径（目前需要将udf发送给各个worker端）
    private String functionClasspath;

    public void setFunctionClasspath(String functionClasspath) {
        this.functionClasspath = functionClasspath;
    }

    public BiOptParamsModel(BinaryExecutionOperator<MODEL> operatorParam, FunctionModel functionModel) {
        this.operatorParam = operatorParam;
        this.functionModel = functionModel;
    }

    public BinaryExecutionOperator<MODEL> getOperatorParam() {
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
