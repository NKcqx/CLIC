package fdu.daslab.service.impl;

import fdu.daslab.scheduler.TaskScheduler;
import fdu.daslab.scheduler.event.TaskSubmitEvent;
import fdu.daslab.scheduler.model.KubernetesStage;
import fdu.daslab.scheduler.model.Task;
import fdu.daslab.thrift.master.TaskService;
import fdu.daslab.util.FieldName;
import org.apache.thrift.TException;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 任务提交的实现类
 *
 * @author 唐志伟, Du Qinghua
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

    //查看所有任务的状态
    @Override
    public List<Map<String, String>> listAllTask() {
        List<Map<String, String>> result = new ArrayList<>();
        List<Task> allTaskList = taskScheduler.handlerListAllEvent();
        allTaskList.forEach(
                task -> {
                    Map<String, String> taskMap = new HashMap<>();
                    taskMap.put(FieldName.TASK_PLAN_NAME, transToString(task.getPlanName()));
                    taskMap.put(FieldName.TASK_SUBMIT_TIME, transToString(task.getSubmitTime()));
                    taskMap.put(FieldName.TASK_START_TIME, transToString((task.getStartTime())));
                    taskMap.put(FieldName.TASK_COMPLETE_TIME, transToString(task.getCompleteTime()));
                    taskMap.put(FieldName.TASK_STATUS, transToString(task.getTaskStatus()));
                    result.add(taskMap);
                }
        );
        return result;
    }

    /**
     * 查看指定task的详细信息
     *
     * @param planName 指定的task
     * @return task info
     * @throws TException
     */
    @Override
    public Map<String, String> getTaskInfo(String planName) throws TException {
        Map<String, String> result = new HashMap<>();
        Task task = taskScheduler.getTaskByTaskName(planName);
        if (task != null) {
            result.put(FieldName.TASK_PLAN_NAME, transToString(task.getPlanName()));
            result.put(FieldName.TASK_SUBMIT_TIME, transToString(task.getSubmitTime()));
            result.put(FieldName.TASK_START_TIME, transToString(task.getStartTime()));
            result.put(FieldName.TASK_COMPLETE_TIME, transToString(task.getCompleteTime()));
            result.put(FieldName.TASK_STATUS, transToString(task.getTaskStatus()));
            List<String> stageIdList = task.getStageIdList();
            int stageNum = stageIdList.size();
            result.put(FieldName.TASK_STAGE_NUM, Integer.toString(stageNum));
            result.put(FieldName.TASK_STAGE_LIST, transToString(stageIdList));
        }
        return result;
    }

    /**
     * 查看stage的详细信息
     *
     * @param stageId 指定的stage的id
     * @return stage info
     * @throws TException
     */
    @Override
    public Map<String, String> getStageInfo(String stageId) throws TException {
        Map<String, String> result = new HashMap<>();
        KubernetesStage stage = taskScheduler.getStageInfoByStageId(stageId);
        if (stage != null) {
            result.put(FieldName.STAGE_ID, transToString(stage.getStageId()));
            result.put(FieldName.STAGE_PLATFORM, transToString(stage.getPlatformName()));
            List<String> parentStageIds = new ArrayList<>(stage.getParentStageIds());
            result.put(FieldName.STAGE_PARENT_LIST, transToString(parentStageIds));
            List<String> childStageIds = new ArrayList<>(stage.getChildStageIds());
            result.put(FieldName.STAGE_CHILDREN_LIST, transToString(childStageIds));
            result.put(FieldName.STAGE_START_TIME, transToString(stage.getStartTime()));
            result.put(FieldName.STAGE_COMPLETE_TIME, transToString(stage.getCompleteTime()));
            result.put(FieldName.STAGE_RETRY_COUNT, transToString(stage.getRetryCounts()));
            result.put(FieldName.STAGE_JOB_INFO, transToString(stage.getJobInfo()));
        }
        return result;
    }

    /**
     * 获取task下的所有stage
     *
     * @param planName 指定task
     * @return
     * @throws TException
     */
    @Override
    public List<String> getStageIdOfTask(String planName) throws TException {
        Task task = taskScheduler.getTaskByTaskName(planName);
        List<String> stageIdList = task.getStageIdList();
        return stageIdList;
    }

    /**
     * 暂停指定的stage
     *
     * @param stageId 指定stage
     * @return false表示stage已启动无法暂停
     * @throws TException
     */
    @Override
    public boolean suspendStage(String stageId) throws TException {
        boolean isSucess = taskScheduler.suspendStage(stageId);
        return isSucess;
    }

    /**
     * 继续已经暂停的指定的stage
     *
     * @param stageId 指定stage
     * @return false 表示启动失败（未被暂停）
     * @throws TException
     */
    @Override
    public boolean continueStage(String stageId) throws TException {
        boolean isSucess = taskScheduler.continueStage(stageId);
        return isSucess;
    }

    /**
     * 查看指定stage 的中间结果
     *
     * @param stageId 指定stage
     * @return 中间结果的路径path
     * @throws TException
     */
    @Override
    public String getStageResult(String stageId) throws TException {
        String stageResPath = taskScheduler.getOutputDataPathByStageId(stageId);
        return stageResPath;
    }

    //仅适用此处的tool 函数： 将一个对象转换成string
    private String transToString(Object obj) {
        String res = null;
        if (obj == null) {
            res = "   -";
        } else {
            if (obj instanceof Date) {
                res = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(obj);
            } else if (obj instanceof List) {
                res = listToString((List<String>) obj);
            } else {
                res = obj.toString();
            }
        }
        return res;
    }

    //仅适用此处的tool 函数： list to string，使用“，”拼接
    private String listToString(List<String> list) {
        if (list == null) {
            return null;
        }
        StringBuilder result = new StringBuilder();
        boolean first = true;
        //第一个前面不拼接","
        for (String string : list) {
            if (first) {
                first = false;
            } else {
                result.append(",");
            }
            result.append(string);
        }
        return result.toString();
    }

}
