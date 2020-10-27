package fdu.daslab.scheduler.model;

/**
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
