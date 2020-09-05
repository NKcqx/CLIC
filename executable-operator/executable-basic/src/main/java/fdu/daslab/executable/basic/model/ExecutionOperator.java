package fdu.daslab.executable.basic.model;


import java.io.Serializable;

/**
 * 底层平台上抽象定义的算子，各个平台之间通用
 *
 * @param <MODEL> 每个平台定义流转的数据模型，比如Stream、RDD
 *
 * @author 唐志伟
 * @since 2020/7/6 12:26 PM
 * @version 1.0
 */
public interface ExecutionOperator<MODEL> extends Serializable {

    /**
     * 算子的执行
     *
     * @param inputArgs 参数列表
     * @param result 返回的结果
     */
    void execute(ParamsModel inputArgs, ResultModel<MODEL> result);
}
