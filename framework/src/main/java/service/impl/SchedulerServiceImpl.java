package service.impl;

import base.ResultCode;
import base.ServiceBaseResult;
import driver.CLICScheduler;
import driver.event.StageCompletedEvent;
import driver.event.StageDataPreparedEvent;
import driver.event.StageStartedEvent;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import service.SchedulerService;

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
    public ServiceBaseResult postStageStarted(int stageId, Map<String, String> submitMsg) throws TException {
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
    public ServiceBaseResult postDataPrepared(int stageId) throws TException {
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
    public ServiceBaseResult postStageCompleted(int stageId, Map<String,
            String> submitMsg) throws TException {
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

    /*======  除了和driver进行交互之外，还可以提供一些额外的接口，方便用户来访问集群的运行状态  ======*/
}
