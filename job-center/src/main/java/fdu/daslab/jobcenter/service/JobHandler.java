package fdu.daslab.jobcenter.service;

import fdu.daslab.thrift.base.Job;
import fdu.daslab.thrift.base.Plan;
import fdu.daslab.thrift.jobcenter.JobService;
import fdu.daslab.jobcenter.client.OptimizerClient;
import fdu.daslab.jobcenter.client.SchedulerClient;
import fdu.daslab.jobcenter.repository.JobRepository;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


/**
 * 任务管理相关的实现类
 *
 * @author 唐志伟
 * @version 1.0
 * @since 5/18/21 3:03 PM
 */
@Service
public class JobHandler implements JobService.Iface{

    @Autowired
    private OptimizerClient optimizerService;

    @Autowired
    private SchedulerClient schedulerService;

    @Autowired
    private JobRepository jobRepository;

    @Override
    public void submit(Plan plan, String jobName) throws TException {
        // 1.调用优化器进行优化，返回 physical plan
        optimizerService.open();
        Job job;
        try {
            job = optimizerService.getClient().optimize(plan);
        } finally {
            optimizerService.close();
        }

        // 2.保存任务，先使用中间状态保存
        job.setJobName(jobName);
        jobRepository.saveJob(job);

        // 3.提交给调度器调度并执行
        schedulerService.open();
        try {
            schedulerService.getClient().schedule(job);
        } finally {
            schedulerService.close();
        }

    }

    @Override
    public Job findJob(String jobName) throws TException {
        return jobRepository.f;
    }

    @Override
    public void updateJob(Job job) throws TException {

    }


}
