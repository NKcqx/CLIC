package fdu.daslab.backend.executor.utils;

import fdu.daslab.backend.executor.model.KubernetesStage;

import java.util.HashMap;
import java.util.Map;

/**
 * kubernetes调用需要的工具类，包含创建pod，获取指定pod对应的ip和port
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/9/28 4:18 PM
 */
public class KubernetesUtil {

    /**
     * 根据argo的文件，生成同时执行的一系列pod，同时保存其物理上的信息（ip, port）并返回
     *
     * @param argoPath argo的Dag路径
     * @return stageId和stageInfo之间的对应关系
     */
    public static Map<Integer, KubernetesStage> createStagePodAndGetStageInfo(String argoPath) {
        Map<Integer, KubernetesStage> stages = new HashMap<>();

        return stages;
    }
}
