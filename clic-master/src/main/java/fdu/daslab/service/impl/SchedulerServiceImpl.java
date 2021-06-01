package fdu.daslab.service.impl;

import fdu.daslab.scheduler.CLICScheduler;
import fdu.daslab.scheduler.event.StageCompletedEvent;
import fdu.daslab.scheduler.event.StageDataPreparedEvent;
import fdu.daslab.scheduler.event.StageStartedEvent;
import fdu.daslab.thrift.base.ResultCode;
import fdu.daslab.thrift.base.ServiceBaseResult;
import fdu.daslab.thrift.master.SchedulerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author 唐志伟
 * @version 1.0
 * @since 2020/9/23 8:10 PM
 */
public class SchedulerServiceImpl implements SchedulerService.Iface {

    private static Logger logger = LoggerFactory.getLogger(SchedulerServiceImpl.class);

    // 调度器
    private CLICScheduler clicScheduler;

    public SchedulerServiceImpl(CLICScheduler clicScheduler) {
        this.clicScheduler = clicScheduler;
    }

    @Override
    public ServiceBaseResult postStageStarted(String stageId, Map<String, String> submitMsg) {
        ServiceBaseResult serviceResult = new ServiceBaseResult();
        try {
            clicScheduler.post(new StageStartedEvent(stageId, submitMsg));
        } catch (Exception e) {
            logger.error(stageId + " post started error: " + e.getMessage());
            serviceResult.setResultCode(ResultCode.EXCEPTION);
            serviceResult.setMessage(e.getMessage());
            return serviceResult;
        }
        serviceResult.setResultCode(ResultCode.SUCCESS);
        return serviceResult;
    }

    @Override
    public ServiceBaseResult postDataPrepared(String stageId) {
        ServiceBaseResult serviceResult = new ServiceBaseResult();
        try {
            clicScheduler.post(new StageDataPreparedEvent(stageId));
        } catch (Exception e) {
            logger.error(stageId + " post data prepared error: " + e.getMessage());
            serviceResult.setResultCode(ResultCode.EXCEPTION);
            serviceResult.setMessage(e.getMessage());
            return serviceResult;
        }
        serviceResult.setResultCode(ResultCode.SUCCESS);
        return serviceResult;
    }

    @Override
    public ServiceBaseResult postStageCompleted(String stageId, Map<String,
            String> submitMsg) {
        ServiceBaseResult serviceResult = new ServiceBaseResult();
        try {
            clicScheduler.post(new StageCompletedEvent(stageId, submitMsg));
        } catch (Exception e) {
            logger.error(stageId + " post stage completed error: " + e.getMessage());
            serviceResult.setResultCode(ResultCode.EXCEPTION);
            serviceResult.setMessage(e.getMessage());
            return serviceResult;
        }
        serviceResult.setResultCode(ResultCode.SUCCESS);
        return serviceResult;
    }
}
