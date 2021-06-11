package fdu.daslab.scheduler.event;

/**
 * 用户提交的任务的事件
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/10/26 10:50 AM
 */
public abstract class TaskEvent {

    private String planName; // 任务的名称

    public TaskEvent(String planName) {
        this.planName = planName;
    }

    public String getPlanName() {
        return planName;
    }

    public void setPlanName(String planName) {
        this.planName = planName;
    }
}
