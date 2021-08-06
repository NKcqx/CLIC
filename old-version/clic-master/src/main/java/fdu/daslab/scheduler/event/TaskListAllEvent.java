package fdu.daslab.scheduler.event;

/**
 * 查询所有已经被提交的任务的状态的事件
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/10/27 9:46 AM
 */
public class TaskListAllEvent extends TaskEvent {
    public TaskListAllEvent() {
        super("ALL");
    }
}
