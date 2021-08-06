package fdu.daslab.optimizercenter.repository;

import fdu.daslab.thrift.base.Plan;
import fdu.daslab.thrift.optimizercenter.OptimizerModel;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author 唐志伟
 * @version 1.0
 * @since 5/18/21 4:40 PM
 */
@Repository
public class OptimizerRepository {

    private List<OptimizerModel> optimizerModels = new ArrayList<>();
    // 保存调用次数
    private Map<String, Integer> callFreq = new HashMap<>();

    // 注册优化器，先简单，保存到内存
    public void registerOptimizer(OptimizerModel optimizerModel) {
        optimizerModels.add(optimizerModel);
    }

    // 查找满足当前计划的所有优化器
    public List<OptimizerModel> filterPossibleOptimizers(Plan plan) {
        return optimizerModels.stream()
                .filter(optimizerModel -> {
                    String platforms = plan.others.getOrDefault("platforms", "");
                    final List<String> platformList = Arrays.asList(platforms.split(","));
                    // 当优化器没有指定平台 或者 优化器指定的平台包含plan中设置的平台
                    return CollectionUtils.isEmpty(optimizerModel.allowedPlatforms)
                            || StringUtils.isEmpty(platforms)
                            || optimizerModel.allowedPlatforms.stream().anyMatch(platformList::contains);
                }).collect(Collectors.toList());
    }

    // optimizer的调用次数
    public void addCallFreq(OptimizerModel optimizerModel) {
        String optimizerName = optimizerModel.name;
        callFreq.put(optimizerName, callFreq.getOrDefault(optimizerName, 0) + 1);
    }
    public OptimizerModel getLeastFreq(List<OptimizerModel> optimizerModels) {
        return optimizerModels.stream().min((model1, model2) ->
                callFreq.get(model2.name) - callFreq.get(model1.name)).orElseThrow(RuntimeException::new);
    }
}
