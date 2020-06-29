package edu.daslab.executable.basic.model;

/**
 * 表示结果
 *
 * MODEL: 不同平台的数据模型，如Stream、RDD等
 */
public interface ResultModel<MODEL> {

    /**
     * 设置result
     *
     * @param result 运行的结果
     */
    void setInnerResult(MODEL result);

    /**
     * 获取运行的结果
     *
     * @return 结果
     */
    MODEL getInnerResult();
}
