package fdu.daslab.scheduler;

import fdu.daslab.scheduler.model.KubernetesStage;
import fdu.daslab.kubernetes.KubernetesUtil;
import fdu.daslab.redis.TaskRepository;
import fdu.daslab.scheduler.event.TaskEvent;
import fdu.daslab.scheduler.event.TaskSubmitEvent;
import fdu.daslab.scheduler.model.StageStatus;
import fdu.daslab.scheduler.model.Task;
import fdu.daslab.scheduler.model.TaskStatus;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static Logger logger = LoggerFactory.getLogger(TaskScheduler.class);

    // TODO: 请求过多时会有问题，后期改成惰性查询/修改
    // 对于task的增删改查
    private TaskRepository taskRepository = new TaskRepository();

    public TaskScheduler(CLICScheduler clicScheduler) {
        super("Task Scheduler");
        this.clicScheduler = clicScheduler;
    }

    @Override
    protected void onReceive(TaskEvent event) {
        if (event instanceof TaskSubmitEvent) {
            // 任务提交
            handlerEventSubmit((TaskSubmitEvent) event);
        }
    }

    /**
     * 处理task提交的逻辑
     *
     * @param event 提交事件
     */
    private void handlerEventSubmit(TaskSubmitEvent event) {
        // 全局唯一的planName
        String uniquePlanName = event.getPlanName() + "-"
                + RandomStringUtils.randomAlphabetic(8).toLowerCase();
        logger.info("Receive plan: " + uniquePlanName + " from " + event.getPlanDagPath());
        // 首先会读取plan中的dag，然后将其切分为不同的stage，并推送给下游处理
        final Map<String, KubernetesStage> stageMap = KubernetesUtil.adaptArgoYamlToKubernetes(
                uniquePlanName, event.getPlanDagPath());
        // 保存当前task
        Collection<KubernetesStage> stages = stageMap.values();
        List<String> stageIdList = stages.stream()
                .map(KubernetesStage::getStageId).collect(Collectors.toList());
        Task newTask = new Task(uniquePlanName, stageIdList);
        taskRepository.addOrUpdateTaskInfo(newTask);
        // 将所有的stage发送给CLICScheduler去实际地执行
        clicScheduler.handlerNewStageList(stages);
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
                        ? curStageStartTime : startTime;
            }
            if (allCompleted && curStageEndTime != null) {
                endTime = (endTime == null || curStageEndTime.after(endTime))
                        ? curStageEndTime : endTime;
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
        taskRepository.addOrUpdateTaskInfo(task);
    }

    /*================以下接口是暴露给用户以方便用户查询内部状态信息=====================*/

    /**
     * 查看所有任务的状态
     */
    public List<Task> handlerListAllEvent() {
        Map<String, Task> taskList = taskRepository.listAllTask();
        // 对于每一个任务，如果没有完成，则查看是否所有的stage都完成了，并更新状态
        List<Task> taskData = new ArrayList<>();
        taskList.forEach((planName, task) -> {
            if (task.getTaskStatus() != TaskStatus.COMPLETED) {
                updateTask(task);
            }
            taskData.add(task);
        });
        return taskData;
    }

    /**
     * 查看某个task的详细信息
     */
    public Task getTaskByTaskName(String taskName) {
        Task task = taskRepository.getTaskInfo(taskName);
        if (task != null && task.getTaskStatus() != TaskStatus.COMPLETED) {
            updateTask(task);
        }
        return task;
    }

    /**
     * 查看某个stage的详细信息
     */
    public KubernetesStage getStageInfoByStageId(String stageId) {
        return clicScheduler.getStageInfo(stageId);
    }

    /**
     * 查看指定stage的输出结果（保存在文件路径下）
     */
    public String getOutputDataPathByStageId(String stageId) {
        final KubernetesStage stageInfo = clicScheduler.getStageInfo(stageId);
        if (!StageStatus.COMPLETED.equals(stageInfo.getStageStatus())) {
            return null;
        }
        return stageInfo.getOutputDataPath();
    }

    /**
     * 暂停指定stage的运行。
     * 这个方法只能暂停还没有开始运行的stage，如果stage已经运行，则返回false
     */
    public boolean suspendStage(String stageId) {
        return clicScheduler.suspendStageByStageId(stageId);
    }

    /**
     * 将暂停的stage重新放到调度队列中去执行
     */
    public boolean continueStage(String stageId) {
        return clicScheduler.continueStageByStageId(stageId);
    }

    /*==================================================================*/

}
