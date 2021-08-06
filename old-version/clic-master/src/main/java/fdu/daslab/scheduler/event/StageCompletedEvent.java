package fdu.daslab.scheduler.event;

import java.util.Map;

/**
 * stage完成
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/9/24 2:38 PM
 */
public class StageCompletedEvent extends SchedulerEvent {
    private Map<String, String> messages;

    public StageCompletedEvent(String stageId, Map<String, String> messages) {
        super(stageId);
        this.messages = messages;
    }
}
