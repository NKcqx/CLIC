package driver.event;

import java.util.Map;

/**
 * stage开始
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/9/24 2:37 PM
 */
public class StageStartedEvent extends SchedulerEvent {
    private Map<String, String> messages;

    public StageStartedEvent(Integer stageId, Map<String, String> messages) {
        super(stageId);
        this.messages = messages;
    }
}
