package fdu.daslab.service.impl;

import fdu.daslab.scheduler.TaskScheduler;
import fdu.daslab.scheduler.event.TaskSubmitEvent;
import fdu.daslab.thrift.master.TaskService;

/**
 *
 * 任务提交的实现类
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/10/23 8:15 PM
 */
public class TaskServiceImpl implements TaskService.Iface {

    private TaskScheduler taskScheduler;

    public TaskServiceImpl(TaskScheduler taskScheduler) {
        this.taskScheduler = taskScheduler;
    }

    @Override
    public void submitPlan(String planName, String planDagPath) {
        try {
            taskScheduler.post(new TaskSubmitEvent(planName, planDagPath));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
