package fdu.daslab.scheduler.event;


/**
 * 标记提交任务
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/10/26 10:51 AM
 */
public class TaskSubmitEvent extends TaskEvent {

    // 实际描述任务的dag的地址
    private String planDagPath;

    public TaskSubmitEvent(String planName, String planDagPath) {
        super(planName);
        this.planDagPath = planDagPath;
    }

    public String getPlanDagPath() {
        return planDagPath;
    }

    public void setPlanDagPath(String planDagPath) {
        this.planDagPath = planDagPath;
    }
}
