package edu.daslab.executable.basic.model;


import java.io.Serializable;

/**
 * 算子
 */
public interface BasicOperator<MODEL> extends Serializable {

    /**
     * 算子的执行
     *
     * @param inputArgs 参数列表
     * @param result 返回的结果
     */
    void execute(ParamsModel<MODEL> inputArgs, ResultModel<MODEL> result);
}
