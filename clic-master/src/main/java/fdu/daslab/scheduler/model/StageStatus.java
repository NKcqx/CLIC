package fdu.daslab.scheduler.model;

/**
 * stage的状态，这里的状态信息都是直接由各个子平台上报
 * <p>
 * stage的状态转移图
 * <p>
 * WAITING  ====> RUNNING  ====> PREPARED ====> COMPLETED
 * ||             ||
 * ||             ||     ====> FAILED
 * ||
 * ||     ====> SUSPEND  ====> WAITING
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/11/10 4:31 PM
 */
public enum StageStatus {

    WAITING, // 在等待队列中等待被执行
    RUNNING, // 正在运行中
    PREPARED, // 数据准备好了
    COMPLETED, // stage已经完成
    FAILED, // 任务失败
    SUSPEND, // 任务被暂停
}
