package fdu.daslab.scheduler;

import fdu.daslab.evaluator.Evaluators;
import fdu.daslab.kubernetes.KubernetesUtil;
import fdu.daslab.redis.StageRepository;
import fdu.daslab.scheduler.event.SchedulerEvent;
import fdu.daslab.scheduler.event.StageCompletedEvent;
import fdu.daslab.scheduler.event.StageDataPreparedEvent;
import fdu.daslab.scheduler.event.StageStartedEvent;
import fdu.daslab.scheduler.model.KubernetesStage;
import fdu.daslab.scheduler.model.StageStatus;
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

    // 不需要保存
    // private Queue<String> waitingQueue = new LinkedList<>();
    // 被暂停的stage
    // 加个并发控制，防止多个线程同时访问stage
//    private Set<String> suspendStages = new ConcurrentSkipListSet<>();
//
//    // 已经完成的stage
//    private Set<String> completedStages = new HashSet<>();
//    // 现在正在运行的stage
//    private Set<String> runningStages = new HashSet<>();
//    // 数据已经准备好的stage
//    private Set<String> dataPreparedStages = new HashSet<>();

    // TODO: 请求过多时会有问题，后期改成惰性查询/修改
    // 所有stage运行需要的信息，因为stage信息会在调度时频繁访问，因此还需要在本地做一个cache
    // 如果做stage的cache，那么1.保存多久的数据 2.在什么时机保持redis和本地的数据一致性
    // 只保存正在运行的任务的stage，运行结束一定会写入redis；以及惰性写入redis，比如用户访问时
//    private Map<String, KubernetesStage> stageIdToStage = new HashMap<>();
    // 对于stage的增啥改查
    private StageRepository stageRepository = new StageRepository();

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
        stageRepository.updateStageStarted(stageId);
    }

    /**
     * 处理stage完成事件
     *
     * @param event 事件信息
     */
    private void handlerCompletedEvent(StageCompletedEvent event) {
        String stageId = event.getStageId();
        logger.info(stageId + " completed!");
        stageRepository.updateStageCompleted(stageId);
//        stageRepository.updateCompleteTime(stageId, new Date());
//        completedStages.add(stageId);
//        runningStages.remove(stageId);
//        dataPreparedStages.remove(stageId);
//        // 打印出当前正在运行的stage
//        for (String runningId : runningStages) {
//            logger.info("Stage " + runningId + " is still running!");
//        }
    }

    private void handlerDataPreparedEvent(StageDataPreparedEvent event) {
        String stageId = event.getStageId();
        stageRepository.updateStageStatus(stageId, StageStatus.PREPARED);
        // 需要调度下一跳执行
//        dataPreparedStages.add(stageId);
        for (String nextStageId : getNextStageIds(stageId)) { // 所有接下来要运行的stage
            logger.info(stageId + " data prepared, start schedule next ===> " + nextStageId);
            schedulerNextStage(nextStageId);
        }
    }

    /**
     * 根据一定的调度策略，调度该stage去执行。
     *
     * step1: 根据该任务，评估该任务所需要的请求资源（cpu、gpu、memory、pod numbers）和任务时间
     *        【策略：算法进行模型估计】
     * step2: 将所有的数据保存到 存储 中，以便底层kubernetes调度时需要
     * step3: 直接创建pod提交到kubernetes上
     * step4: kubernetes会底层维护一个SchedulerQueue，并根据调度队列调度指定的pod
     *        【需要基于kubernetes的Scheduler Framework来扩展一个scheduler算法】
     *
     * @param nextStageId 即将被调度的stageId
     */
    private void schedulerNextStage(String nextStageId) {
        KubernetesStage stage = stageRepository.getStageInfo(nextStageId);
        // 对于暂停的stage，不执行
        if (StageStatus.SUSPEND.equals(stage.getStageStatus())) {
            logger.warn(nextStageId + " is suspended! ");
            return;
        }
        // 不做任何处理，直接创建job，具体的调度逻辑放到k8s的scheduler framework中完成
        Evaluators.evaluate(stage);
        KubernetesUtil.submitJobStage(stage.getJobInfo());
    }

    // 获取该stage的下一个运行的stage，这些stage必须都已经完成，或者数据准备完成
    private Set<String> getNextStageIds(String stageId) {
        Set<String> result = new HashSet<>();
        // 缺少非空判断，暂时默认都一定存在（合理）
        stageRepository.getStageInfo(stageId).getChildStageIds().forEach(childStageId -> {
            // 所有的stage都准备好了
            if (checkAllDependencies(childStageId)) {
                result.add(childStageId);
            }
        });
        return result;
    }

    // 判断是否该stageId的前置依赖是否都已经完成
    private boolean checkAllDependencies(String stageId) {
        return stageRepository.getStageInfo(stageId)
                .getParentStageIds()
                .stream()
                .allMatch(pid -> {
                    KubernetesStage parentStage = stageRepository.getStageInfo(pid);
                    StageStatus parentStatus = parentStage != null ? parentStage.getStageStatus() : null;
                    return StageStatus.COMPLETED.equals(parentStatus)
                            || StageStatus.PREPARED.equals(parentStatus);
                });
    }

    /*================以下接口是暴露给上层TaskScheduler以完成某种功能=====================*/

    /**
     * 处理新过来的所有的stages
     *
     * @param stages 所有的stage
     */
    public void handlerNewStageList(Collection<KubernetesStage> stages) {
        // 找到初始的stage，其余的stage全部加入到待处理的队列中
        stageRepository.addStageBatch(stages);
//        stageIdToStage.putAll(stageMap);
        stages.forEach(stage -> {
            // 没有依赖的stage为初始的stage，先进行调度
            String stageId = stage.getStageId();
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
        return stageRepository.getStageInfo(stageId);
    }

    /**
     * 暂停指定stage的运行，不允许对正在运行 或者 运行结束的stage 执行该操作，因为这样会影响效果
     *
     * @param stageId stage的唯一标识
     * @return 暂停成功 / 失败
     */
    public boolean suspendStageByStageId(String stageId) {
        KubernetesStage stage = stageRepository.getStageInfo(stageId);
        // 只允许暂停还没开始的stage
        if (!StageStatus.WAITING.equals(stage.getStageStatus())) {
            return false;
        }
        stageRepository.updateStageStatus(stageId, StageStatus.SUSPEND);
//        if (!stageIdToStage.containsKey(stageId) || completedStages.contains(stageId)
//            || runningStages.contains(stageId) || dataPreparedStages.contains(stageId)) {
//            // 对于所有正在运行 / 已经完成的stage，不允许暂停
//            return false;
//        }
//        suspendStages.add(stageId);
        return true;
    }

    /**
     * 重启指定stage的运行
     *
     * @param stageId stage的唯一标识
     * @return 重启成功 / 失败
     */
    public boolean continueStageByStageId(String stageId) {
        KubernetesStage stage = stageRepository.getStageInfo(stageId);
        if (!StageStatus.SUSPEND.equals(stage.getStageStatus())) {
            return false;
        }
        stageRepository.updateStageStatus(stageId, StageStatus.WAITING);
//        if (!suspendStages.contains(stageId)) {
//            return false;
//        }
//        suspendStages.remove(stageId);
        // 如果其依赖的stage都已经完成了或者准备好了，需要执行该stage
        if (checkAllDependencies(stageId)) {
            schedulerNextStage(stageId);
        }
        return true;
    }

    /*=================================================================*/

}
