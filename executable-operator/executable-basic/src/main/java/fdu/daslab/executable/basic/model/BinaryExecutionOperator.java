package fdu.daslab.executable.basic.model;

import java.io.Serializable;

/**
 * @author 唐志伟，刘丰艺
 * @since 2020/7/6 14:05
 * @version 1.0
 */
public interface BinaryExecutionOperator<MODEL> extends Serializable {

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
