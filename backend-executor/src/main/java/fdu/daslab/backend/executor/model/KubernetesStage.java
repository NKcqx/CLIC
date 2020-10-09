package fdu.daslab.backend.executor.model;

import io.kubernetes.client.openapi.models.V1Pod;

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

    Integer stageId;
    //        String dagPath;
//        String udfPath;
    String host;    // 运行的host，需要生成后在才会存在
    Integer port; // 运行的port，指的是thrift的port，用于不同服务交互 TODO: port是否需要在k8s中开放
    Integer retryCounts; // 重试次数，重试最多三次
    Set<Integer> parentStageIds = new HashSet<>();    // 所依赖的父stage
    Set<Integer> childStageIds = new HashSet<>();     // 依赖本stage的child stage
    V1Pod podInfo; // 实际的pod的定义信息

    public KubernetesStage(Integer stageId) {
        this.stageId = stageId;
    }

    public V1Pod getPodInfo() {
        return podInfo;
    }

    public void setPodInfo(V1Pod podInfo) {
        this.podInfo = podInfo;
    }

    public Integer getStageId() {
        return stageId;
    }

    public void setStageId(Integer stageId) {
        this.stageId = stageId;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public Integer getRetryCounts() {
        return retryCounts;
    }

    public void setRetryCounts(Integer retryCounts) {
        this.retryCounts = retryCounts;
    }

    public Set<Integer> getParentStageIds() {
        return parentStageIds;
    }

    public void setParentStageIds(Set<Integer> parentStageIds) {
        this.parentStageIds = parentStageIds;
    }

    public Set<Integer> getChildStageIds() {
        return childStageIds;
    }

    public void setChildStageIds(Set<Integer> childStageIds) {
        this.childStageIds = childStageIds;
    }
}
