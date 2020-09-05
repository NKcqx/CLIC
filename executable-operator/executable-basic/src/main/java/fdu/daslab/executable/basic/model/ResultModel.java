package fdu.daslab.executable.basic.model;

/**
 * 平台的运行结果
 *
 * @param <MODEL> 每个平台定义流转的数据模型，比如Stream、RDD
 *
 * @author 唐志伟
 * @since 2020/7/6 1:35 PM
 * @version 1.0
 */
public interface ResultModel<MODEL> {

    /**
     * 设置result
     *
     * @param result 运行的结果
     */
    void setInnerResult(String key, MODEL result);

    /**
     * 获取运行的结果
     *
     * @return 结果
     */
    MODEL getInnerResult(String key);
}
