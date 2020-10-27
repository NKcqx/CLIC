package fdu.daslab.scheduler;

import fdu.daslab.backend.executor.model.KubernetesStage;
import fdu.daslab.backend.executor.utils.YamlUtil;
import fdu.daslab.scheduler.event.TaskEvent;
import fdu.daslab.scheduler.event.TaskListAllEvent;
import fdu.daslab.scheduler.event.TaskSubmitEvent;
import fdu.daslab.scheduler.model.Task;
import fdu.daslab.scheduler.model.TaskStatus;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 任务 / plan 粒度的调度器，负责接收粗粒度的任务，然后交给底层的ClicScheduler进行处理
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/10/26 10:06 AM
 */
public class TaskScheduler extends EventLoop<TaskEvent> {

    private CLICScheduler clicScheduler;

    // 保存当前的所有plan
    private Map<String, Task> taskList = new HashMap<>();

    public TaskScheduler(CLICScheduler clicScheduler) {
        super("Task Scheduler");
        this.clicScheduler = clicScheduler;
    }

    @Override
    protected void onReceive(TaskEvent event) {
        if (event instanceof TaskSubmitEvent) {
            // 任务提交
            handlerEventSubmit((TaskSubmitEvent) event);
        } else if (event instanceof TaskListAllEvent) {
            // 查询所有正在运行的任务的状态
            handlerListAllEvent();
        }
    }

    /**
     * 处理task提交的逻辑
     *
     * @param event 提交事件
     */
    private void handlerEventSubmit(TaskSubmitEvent event) {
        // 全局唯一的planName
        String uniquePlanName = event.getPlanName() + RandomStringUtils.randomAlphabetic(6);
        // 首先会读取plan中的dag，然后将其切分为不同的stage，并推送给下游处理
        final Map<String, KubernetesStage> stages = YamlUtil.adaptArgoYamlToKubernetes(
                uniquePlanName, event.getPlanDagPath());
        // 保存当前task
        List<String> stageIdList = stages.values().stream()
                .map(KubernetesStage::getStageId).collect(Collectors.toList());
        Task newTask = new Task(uniquePlanName, stageIdList);
        taskList.put(uniquePlanName, newTask);
        // 将所有的stage发送给CLICScheduler去实际地执行
        clicScheduler.handlerNewStageList(stages);
    }

    /**
     * 查看所有任务的状态
     */
    private void handlerListAllEvent() {
        // 对于每一个任务，如果没有完成，则查看是否所有的stage都完成了，并更新状态
        taskList.forEach((planName, task) -> {
            if (task.getTaskStatus() != TaskStatus.COMPLETED) {
                updateTask(task);
            }
        });
    }

    /**
     * task的状态是惰性更新的，开始时间取决于最先开始的stage的时间，结束时间取决于最后结束的stage的时间
     *
     * @param task task
     */
    private void updateTask(Task task) {
        // 查询所有stage的状态，并更新task的状态
        Date startTime = null, endTime = null;
        boolean allCompleted = true;
        for (String stageId : task.getStageIdList()) {
            // 调用clicScheduler查看各个stage的状态
            KubernetesStage stage = clicScheduler.getStageInfo(stageId);
            Date curStageStartTime = stage.getStartTime();
            Date curStageEndTime = stage.getCompleteTime();
            if (curStageStartTime != null) {
                startTime = (startTime == null || curStageStartTime.before(startTime))
                        ? curStageStartTime : null;
            }
            if (allCompleted && curStageEndTime != null) {
                endTime = (endTime == null || curStageEndTime.after(endTime))
                        ? curStageEndTime : null;
            } else {
                allCompleted = false;
            }
        }
        if (allCompleted) {
            // 所有stage都完成
            task.setStartTime(startTime);
            task.setCompleteTime(endTime);
            task.setTaskStatus(TaskStatus.COMPLETED);
        } else if (startTime != null) {
            // 已经开始
            task.setStartTime(startTime);
            task.setTaskStatus(TaskStatus.RUNNING);
        }
    }
}
