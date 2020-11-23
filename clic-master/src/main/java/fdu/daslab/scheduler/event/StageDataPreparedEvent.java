package fdu.daslab.scheduler.event;

/**
 * stage数据准备完成
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/9/24 2:38 PM
 */
public class StageDataPreparedEvent extends SchedulerEvent {

    public StageDataPreparedEvent(String stageId) {
        super(stageId);
    }
}
