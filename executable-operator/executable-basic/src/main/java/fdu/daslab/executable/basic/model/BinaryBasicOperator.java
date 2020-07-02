package fdu.daslab.executable.basic.model;

import java.io.Serializable;

/**
 * Bi类型算子
 */
public interface BinaryBasicOperator<PARAM, INPUT1, INPUT2> extends Serializable {

    /**
     * 算子的执行
     *
     * @param inputArgs 参数列表
     * @param input1 第一条流
     * @param input2 第二条流
     */
    void execute(ParamsModel<PARAM> inputArgs,
                 ResultModel<INPUT1> input1,
                 ResultModel<INPUT2> input2);
}
