package fdu.daslab.scheduler.event;

/**
 * 调度事件的标记类
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/9/24 2:36 PM
 */
public abstract class SchedulerEvent {
    private String stageId; // stageId

    public SchedulerEvent(String stageId) {
        this.stageId = stageId;
    }

    public String getStageId() {
        return stageId;
    }
}
