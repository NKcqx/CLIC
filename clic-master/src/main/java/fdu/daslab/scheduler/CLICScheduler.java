package fdu.daslab.scheduler;

import fdu.daslab.scheduler.event.SchedulerEvent;
import fdu.daslab.scheduler.event.StageCompletedEvent;
import fdu.daslab.scheduler.event.StageDataPreparedEvent;
import fdu.daslab.scheduler.event.StageStartedEvent;
import fdu.daslab.backend.executor.model.KubernetesStage;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * CLIC的调度器，是一个事件循环，接收stage粒度的调度，此调度器不区分任务概念，做为最底层的执行引擎
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/9/24 10:29 AM
 */
public class CLICScheduler extends EventLoop<SchedulerEvent> {

    private static Logger logger = LoggerFactory.getLogger(CLICScheduler.class);
    // TODO: 以下信息暂时存储在内存中，未来可能统一收敛到etcd来存储state信息
    // 已经完成的stage
    private Set<String> completedStages = new HashSet<>();
    // 现在正在运行的stage
    private Set<String> runningStages = new HashSet<>();
    // 数据已经准备好的stage
    private Set<String> dataPreparedStages = new HashSet<>();
    // 所有stage运行需要的信息
    private Map<String, KubernetesStage> stageIdToStage = new HashMap<>();

    public CLICScheduler() {
        super("CLICScheduler");
    }

    // 执行事件的具体逻辑
    @Override
    protected void onReceive(SchedulerEvent event) {
        if (event instanceof StageStartedEvent) {
            handlerStartedEvent((StageStartedEvent) event);
        } else if (event instanceof StageCompletedEvent) {
            handlerCompletedEvent((StageCompletedEvent) event);
        } else if (event instanceof StageDataPreparedEvent) {
            handlerDataPreparedEvent((StageDataPreparedEvent) event);
        }
    }

    /**
     * 处理stage开始事件
     *
     * @param event 事件信息
     */
    private void handlerStartedEvent(StageStartedEvent event) {
        String stageId = event.getStageId();
        logger.info(stageId + " started!");
        stageIdToStage.get(stageId).setStartTime(new Date());
        runningStages.add(stageId);
    }

    /**
     * 处理stage完成事件
     *
     * @param event 事件信息
     */
    private void handlerCompletedEvent(StageCompletedEvent event) {
        String stageId = event.getStageId();
        logger.info(stageId + " completed!");
        stageIdToStage.get(stageId).setCompleteTime(new Date());
        completedStages.add(stageId);
        runningStages.remove(stageId);
        dataPreparedStages.remove(stageId);
        // 打印出当前正在运行的stage
        for (String runningId : runningStages) {
            logger.info("Stage " + runningId + " is still running!");
        }
    }

    private void handlerDataPreparedEvent(StageDataPreparedEvent event) {
        String stageId = event.getStageId();
        // 需要调度下一跳执行
        dataPreparedStages.add(stageId);
        for (String nextStageId : getNextStageIds(stageId)) { // 所有接下来要运行的stage
            logger.info(stageId + " data prepared, start schedule next ===> " + nextStageId);
            schedulerNextStage(nextStageId);
        }
    }

    /**
     * 根据一定的调度策略，调度该stage去执行，
     *
     * @param nextStageId 即将被调度的stageId
     */
    private void schedulerNextStage(String nextStageId) {
        // 根据当前的任务情况，创建job

        // 实际的调度策略
//        new Thread(() -> {
//            // TODO: 改成直接动态创建pod，而不是调用rpc ===> 存在问题，如何查看运行pod的状态？
//            // 调用对应stage的rpc执行调度
//            KubernetesStage stage = stageIdToStage.get(nextStageId);
//            ExecuteServiceClient stageClient = new ExecuteServiceClient(stage.getHost(), stage.getPort());
//            try {
//                // TODO: 未来参数可能都收敛到这里
//                stageClient.executeStage(new TransParams());
//            } catch (Exception e) {
//                logger.info("Call client of stage" + nextStageId + " fail: " + e.getMessage());
//                // 需要重试
//            }
//
//        }, nextStageId + " listenThread").start();
    }

    // 获取该stage的下一个运行的stage，这些stage必须都已经完成，或者数据准备完成
    private Set<String> getNextStageIds(String stageId) {
        Set<String> result = new HashSet<>();
        stageIdToStage.get(stageId).getChildStageIds().forEach(childStageId -> {
            // 所有的stage都准备好了
            if (stageIdToStage.get(childStageId).getParentStageIds().stream().allMatch(
                    pid -> completedStages.contains(pid) || dataPreparedStages.contains(pid))) {
                result.add(childStageId);
            }
        });
        return result;
    }

    /**
     * 处理新过来的所有的stages
     *
     * @param stageMap 所有的stage
     */
    public void handlerNewStageList(Map<String, KubernetesStage> stageMap) {
        // 找到初始的stage，其余的stage全部加入到待处理的队列中
        stageIdToStage.putAll(stageMap);
        stageMap.forEach((stageId, stage) -> {
            // 没有依赖的stage为初始的stage，先进行调度
            if (CollectionUtils.isEmpty(stage.getParentStageIds())) {
                logger.info("source stage" + stageId + " will be scheduled");
                schedulerNextStage(stageId);
            }
        });

    }

    /**
     * 查看stage的实时的状态，主要是对外部接口访问的需要
     *
     * @param stageId stage的唯一标识
     * @return stage的信息
     */
    public KubernetesStage getStageInfo(String stageId) {
        return stageIdToStage.get(stageId);
    }

}
