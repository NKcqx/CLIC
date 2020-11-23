package fdu.daslab.scheduler.model;

import io.kubernetes.client.openapi.models.V1Job;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * kubernetes中的stage，一个stage对应一个平台，保存其物理上的信息
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/9/29 9:36 AM
 */
public class KubernetesStage {

    private String stageId; // 全局唯一的stageId
    private String platformName; // 对应的平台
//    private String host;    // 运行的host，需要生成后在才会存在
//    private Integer port; // 运行的port，指的是thrift的port，用于不同服务交互
    private Integer retryCounts = 0; // 重试次数，重试最多三次
    private Set<String> parentStageIds = new HashSet<>();    // 所依赖的父stage
    private Set<String> childStageIds = new HashSet<>();     // 依赖本stage的child stage
    private V1Job jobInfo; // 实际的job的定义信息
    private Date startTime;  // stage的开始时间
    private Date completeTime; // stage的完成时间

    // Stage的状态
    private StageStatus stageStatus = StageStatus.WAITING;

    // 输入的路径和输出的路径，默认会带有协议，比如file:// hdfs://
    // TODO: 所有中间结果，不管使用什么传输方式，都需要落盘，一方面方便回溯，一方面也方便用于展示
    // 对于高性能读取的方法，比如RDMA等，采用异步的方式，先传输，同时还需要异步地写入文件
    // 对于hdfs，分布式存的时候是存在一个文件夹中，如何解决？
    private String outputDataPath;

    public KubernetesStage(String stageId) {
        this.stageId = stageId;
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

    public String getPlatformName() {
        return platformName;
    }

    public void setPlatformName(String platformName) {
        this.platformName = platformName;
    }

    public V1Job getJobInfo() {
        return jobInfo;
    }

    public void setJobInfo(V1Job jobInfo) {
        this.jobInfo = jobInfo;
    }

    public String getStageId() {
        return stageId;
    }

    public void setStageId(String stageId) {
        this.stageId = stageId;
    }

//    public String getHost() {
//        return host;
//    }
//
//    public void setHost(String host) {
//        this.host = host;
//    }
//
//    public Integer getPort() {
//        return port;
//    }
//
//    public void setPort(Integer port) {
//        this.port = port;
//    }

    public Integer getRetryCounts() {
        return retryCounts;
    }

    public void setRetryCounts(Integer retryCounts) {
        this.retryCounts = retryCounts;
    }

    public Set<String> getParentStageIds() {
        return parentStageIds;
    }

    public void setParentStageIds(Set<String> parentStageIds) {
        this.parentStageIds = parentStageIds;
    }

    public Set<String> getChildStageIds() {
        return childStageIds;
    }

    public void setChildStageIds(Set<String> childStageIds) {
        this.childStageIds = childStageIds;
    }

    public String getOutputDataPath() {
        return outputDataPath;
    }

    public void setOutputDataPath(String outputDataPath) {
        this.outputDataPath = outputDataPath;
    }

    public StageStatus getStageStatus() {
        return stageStatus;
    }

    public void setStageStatus(StageStatus stageStatus) {
        this.stageStatus = stageStatus;
    }
}
