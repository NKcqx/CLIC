package driver;

import base.TransParams;
import driver.event.SchedulerEvent;
import driver.event.StageCompletedEvent;
import driver.event.StageDataPreparedEvent;
import driver.event.StageStartedEvent;
import fdu.daslab.backend.executor.model.KubernetesStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import service.client.ExecuteServiceClient;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * CLIC的调度器，是一个事件循环
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/9/24 10:29 AM
 */
public class CLICScheduler extends EventLoop<SchedulerEvent> {

    private static Logger logger = LoggerFactory.getLogger(CLICScheduler.class);
    // 已经完成的stage
    private Set<Integer> completedStages = new HashSet<>();
    // 现在正在运行的stage
    private Set<Integer> runningStages = new HashSet<>();
    // 数据已经准备好的stage
    private Set<Integer> dataPreparedStages = new HashSet<>();
    // 所有stage运行需要的信息
    private Map<Integer, Stage> stageIdToStage = new HashMap<>();

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
        Integer stageId = event.getStageId();
        // 暂时不做任何处理，仅打出日志
        logger.info(stageId + " started!");
        runningStages.add(stageId);
    }

    /**
     * 处理stage完成事件
     *
     * @param event 事件信息
     */
    private void handlerCompletedEvent(StageCompletedEvent event) {
        Integer stageId = event.getStageId();
        // 暂时不做任何处理，仅打出日志
        logger.info(stageId + " completed!");
        completedStages.add(stageId);
        runningStages.remove(stageId);
        dataPreparedStages.remove(stageId);
        // 打印出当前正在运行的stage
        for (Integer runningId : runningStages) {
            logger.info("Stage " + runningId + " is still running!");
        }
        // 所有的stage都运行成功，结束事件循环
        if (completedStages.size() == stageIdToStage.size()) {
            logger.info("All stages have completed!");
            stop();
        }
    }

    private void handlerDataPreparedEvent(StageDataPreparedEvent event) {
        Integer stageId = event.getStageId();
        // 需要调度下一跳执行，单独启一个线程去执行，因为这个执行很可能会阻塞一段时间
        dataPreparedStages.add(stageId);
        Set<Integer> nextStageIds = getNextStageIds(stageId); // 所有接下来要运行的stage
        for (Integer nextStageId : nextStageIds) {
            logger.info(stageId + " data prepared, start next ===> " + nextStageId);
            new Thread(() -> {
                // 调用对应stage的rpc执行调度
                Stage stage = stageIdToStage.get(nextStageId);
                ExecuteServiceClient stageClient = new ExecuteServiceClient(stage.host, stage.port);
                try {
                    // TODO: 未来参数可能都收敛到这里
                    stageClient.executeStage(new TransParams());
                } catch (Exception e) {
                    logger.info("Call client of stage" + nextStageId + " fail: " + e.getMessage());
                    // 需要重试
                }

            }, nextStageId + " listenThread").start();
        }
    }

    // 获取该stage的下一个运行的stage，这些stage必须都已经完成，或者数据准备完成
    private Set<Integer> getNextStageIds(Integer stageId) {
        Set<Integer> result = new HashSet<>();
        stageIdToStage.get(stageId).childStageIds.forEach(childStageId -> {
            // 所有的stage都准备好了
            if (stageIdToStage.get(childStageId).parentStageIds.stream().allMatch(
                    pid -> completedStages.contains(pid) || dataPreparedStages.contains(pid))) {
                result.add(childStageId);
            }
        });
        return result;
    }

    // physical上的stage，只包含启动各个平台的stage的运行需要的信息
    public static class Stage {
        Integer stageId;
        String host;    // 运行的host，需要生成后在才会存在
        Integer port;
        Integer retryCounts; // 重试次数，重试最多三次
        Set<Integer> parentStageIds = new HashSet<>();    // 所依赖的父stage
        Set<Integer> childStageIds = new HashSet<>();     // 依赖本stage的child stage
    }

    // 运行侧的stage和这里的stage结构相同，只是为了解耦和划分业务的方便才把Stage结构放到这里
    public void initStages(Map<Integer, KubernetesStage> stageInfos) {
        stageInfos.forEach((stageId, stageInfo) -> {
            Stage stage = new Stage();
            stage.stageId = stageId;
            stage.host = stageInfo.getHost();
            stage.port = stageInfo.getPort();
            stage.retryCounts = 0;
            stage.parentStageIds = stageInfo.getParentStageIds();
            stage.childStageIds = stageInfo.getChildStageIds();
            stageIdToStage.put(stageId, stage);
        });
    }

    // 启动初始的节点
    public void handlerSourceStages() {
        // 所有stage中没有parent依赖的stage
        stageIdToStage.forEach((stageId, stage) -> {
            if (stage.parentStageIds.isEmpty()) {
                // 提交这些初始的stage
                try {
                    post(new StageDataPreparedEvent(stageId));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

}
