package fdu.daslab.executorcenter.kubernetes;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author 唐志伟
 * @version 1.0
 * @since 6/6/21 3:12 PM
 */
@Service
public class StrategyFactory {

    @Autowired
    private Map<String, KubernetesResourceStrategy> strategyMap = new ConcurrentHashMap<>();

    // 根据对应的平台，选择不同的创建方式
    public KubernetesResourceStrategy getStrategyForPlatform(String platform) {
        KubernetesResourceStrategy strategy = strategyMap.get(platform.toLowerCase() + "Operator");
        if (strategy == null) {
            // 默认的情况，走创建job
            strategy = strategyMap.get("kubernetesJob");
        }
        return strategy;
    }
}
