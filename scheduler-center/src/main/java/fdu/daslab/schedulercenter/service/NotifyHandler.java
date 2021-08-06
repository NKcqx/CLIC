package fdu.daslab.schedulercenter.service;

import fdu.daslab.schedulercenter.scheduling.StageScheduling;
import fdu.daslab.thrift.notifyservice.NotifyService;
import fdu.daslab.thrift.notifyservice.StageSnapshot;
import fdu.daslab.thrift.notifyservice.StageStatus;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author 唐志伟
 * @version 1.0
 * @since 6/9/21 3:29 PM
 */
@Service
public class NotifyHandler implements NotifyService.Iface {

    @Autowired
    private StageScheduling stageScheduling;

    @Override
    public void postStatus(String jobName, int stageId, StageSnapshot stageSnapShot) throws TException {
        stageScheduling.updateStatus(jobName, stageId, stageSnapShot);
    }
}
