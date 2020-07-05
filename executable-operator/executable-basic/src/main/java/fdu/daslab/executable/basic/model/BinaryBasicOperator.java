package fdu.daslab.executable.basic.model;

import java.io.Serializable;

/**
 * Bi类型算子
 */
public interface BinaryBasicOperator<MODEL> extends Serializable {

    /**
     * 算子的执行
     *
     * @param inputArgs 参数列表
     * @param input1 第一条输入流
     * @param input2 第二条输入流
     */
    void execute(BiOptParamsModel<MODEL> inputArgs,
                 ResultModel<MODEL> input1,
                 ResultModel<MODEL> input2,
                 ResultModel<MODEL> result);
}
