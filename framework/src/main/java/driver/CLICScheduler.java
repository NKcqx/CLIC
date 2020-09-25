package driver;

import driver.event.SchedulerEvent;
import driver.event.StageCompletedEvent;
import driver.event.StageDataPreparedEvent;
import driver.event.StageStartedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * CILC的调度器，是一个事件循环
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/9/24 10:29 AM
 */
public class CLICScheduler extends EventLoop<SchedulerEvent> {

    private static Logger logger = LoggerFactory.getLogger(CLICScheduler.class);

    public CLICScheduler() {
        super("CLICScheduler");
    }

    // 执行事件的具体逻辑
    @Override
    protected void onReceive(SchedulerEvent event) {
       if (event instanceof StageStartedEvent) {
           // 暂时不做任何处理，仅打出日志
           logger.info(event.getStageId() + " started!");
       } else if (event instanceof StageCompletedEvent) {
           // 暂时不做任何处理，仅打出日志
           logger.info(event.getStageId() + " completed!");
       } else if (event instanceof StageDataPreparedEvent) {
           // 需要调度下一跳执行
           logger.info(event.getStageId() + " data prepared, start next ===> ");
           Integer stageId = 10; // 需要获取下一跳的stageId

       }
    }

}
