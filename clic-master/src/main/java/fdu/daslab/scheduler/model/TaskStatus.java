package fdu.daslab.scheduler.model;

/**
 * task的状态，状态变更图
 * <p>
 * NEW ===> RUNNING ====> COMPLETED
 * ||
 * ||   ====> FAILED
 * NEW: 刚提交
 * RUNNING: Task中至少有一个stage正在运行了
 * COMPLETED: Task的所有stage都完成了
 * FAILED: 执行失败  ===> TODO: 依赖底层的实现，后面考虑
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/10/26 4:16 PM
 */
public enum TaskStatus {

    NEW, // 刚提交等待被调度
    RUNNING, // 正在运行中
    COMPLETED, // 任务已完成
    FAILED, // 任务失败
}
