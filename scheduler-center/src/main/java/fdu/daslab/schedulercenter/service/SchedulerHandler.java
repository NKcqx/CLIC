package fdu.daslab.schedulercenter.service;

import fdu.daslab.schedulercenter.repository.SchedulerRepository;
import fdu.daslab.schedulercenter.scheduling.StageScheduling;
import fdu.daslab.thrift.base.Job;
import fdu.daslab.thrift.base.Stage;
import fdu.daslab.thrift.schedulercenter.SchedulerModel;
import fdu.daslab.thrift.schedulercenter.SchedulerService;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author 唐志伟
 * @version 1.0
 * @since 5/18/21 7:50 PM
 */
@Service
public class SchedulerHandler implements SchedulerService.Iface {

    @Autowired
    private SchedulerRepository schedulerRepository;

    @Autowired
    private StageScheduling stageScheduling;

    @Override
    public void registerScheduler(SchedulerModel schedulerInfo) throws TException {
        schedulerRepository.saveScheduler(schedulerInfo);
    }

    @Override
    public void schedule(Job job) throws TException {
        // 获取初始的stage
        List<Stage> sourceStages = new ArrayList<>();
        job.getSourceStages().forEach(sourceId -> sourceStages.add(job.subplans.get(sourceId)));
        stageScheduling.cacheJob(job);
        stageScheduling.schedule(sourceStages);
    }
}
