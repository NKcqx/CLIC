package fdu.daslab.scheduler.model;


import java.util.Date;
import java.util.List;

/**
 * 描述一个具体的任务（workflow）
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/10/26 4:13 PM
 */
public class Task {

    // plan的名称
    private String planName = null;
    // 任务提交的日期
    private Date submitTime = null;
    // 任务的开始时间
    private Date startTime = null;
    // 任务的完成时间
    private Date completeTime = null;
    // 任务的状态
    private TaskStatus taskStatus = null;

    // 任务包含的stage的id
    private List<String> stageIdList;

    public Task(String planName, List<String> stageList) {
        this.planName = planName;
        this.stageIdList = stageList;
        this.submitTime = new Date();
        this.taskStatus = TaskStatus.NEW;
    }

    public String getPlanName() {
        return planName;
    }

    public void setPlanName(String planName) {
        this.planName = planName;
    }

    public Date getSubmitTime() {
        return submitTime;
    }

    public void setSubmitTime(Date submitTime) {
        this.submitTime = submitTime;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getCompleteTime() {
        return completeTime;
    }

    public void setCompleteTime(Date completeTime) {
        this.completeTime = completeTime;
    }

    public TaskStatus getTaskStatus() {
        return taskStatus;
    }

    public void setTaskStatus(TaskStatus taskStatus) {
        this.taskStatus = taskStatus;
    }

    public List<String> getStageIdList() {
        return stageIdList;
    }

    public void setStageIdList(List<String> stageIdList) {
        this.stageIdList = stageIdList;
    }
}
