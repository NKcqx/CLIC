package fdu.daslab.optimizercenter.service;

import fdu.daslab.optimizercenter.channel.ChannelEnrich;
import fdu.daslab.optimizercenter.client.OperatorClient;
import fdu.daslab.optimizercenter.client.OptimizerPluginClient;
import fdu.daslab.optimizercenter.fusion.OperatorFusion;
import fdu.daslab.optimizercenter.repository.OptimizerRepository;
import fdu.daslab.thrift.base.Job;
import fdu.daslab.thrift.base.Plan;
import fdu.daslab.thrift.base.Platform;
import fdu.daslab.thrift.optimizercenter.OptimizerModel;
import fdu.daslab.thrift.optimizercenter.OptimizerService;
import org.apache.thrift.TException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author 唐志伟
 * @version 1.0
 * @since 5/18/21 4:29 PM
 */
@Service
public class OptimizerHandler implements OptimizerService.Iface {

    @Autowired
    private OptimizerRepository optimizerRepository;

    @Autowired
    private ChannelEnrich channelEnrich;

    @Autowired
    private OperatorFusion operatorFusion;

    @Resource
    private OperatorClient operatorClient;

    // 将优化器的信息保存到注册中心
    @Override
    public void registerOptimizer(OptimizerModel optimizerInfo) {
        optimizerRepository.registerOptimizer(optimizerInfo);
    }

    /**
     *
     * 优化器一个执行计划：
     *             1.查询所有满足这一个查询计划的所有optimizer
     *             2.按照优先级高低，依次对optimizer进行优化
     *             3.channel enrich: 在不同的operator之间添加channel 算子，一般是source 和 sink算子
     *             4.operator fusion：将相邻的operator合并到一个sub plan中
     *           存在问题：
     *             optimizer是否存在互斥，不能使用多个optimizer进行优化
     *                 ===> 不可能，需要保证每一个优化器是 正确的 ，也就是 优化后的执行计划 和 优化前 是一致的
     * @param plan 逻辑计划
     * @return 优化后的物理执行计划
     * @throws TException thrift exception
     */
    @Override
    public Job optimize(Plan plan) throws TException {
        // 查询所有可能的优化器
        List<OptimizerModel> optimizerModelList = optimizerRepository.filterPossibleOptimizers(plan);
        // 首先对优化器按照优先级排序，然后对具有相同优化器的
        optimizerModelList.sort(Comparator.comparingInt(OptimizerModel::getPriority));
        TreeMap<Integer, List<OptimizerModel>> sortedGroupedModel = optimizerModelList.stream()
                .collect(Collectors.groupingBy(OptimizerModel::getPriority, TreeMap::new, Collectors.toList()));
        // 先需要查询所有的可能的platform信息，相当于给优化器提供一个候选
        Map<String, Platform> platforms;
        try {
            operatorClient.open();
            platforms = operatorClient.getClient().listPlatforms();
        } finally {
            operatorClient.close();
        }

        // optimize
        // 对于相同优先级的，只会使用其中一个优化器，优先使用规定了平台的，如果都满足，使用负载均衡策略打到不同的优化器
        for (List<OptimizerModel> modelList : sortedGroupedModel.values()) {
            // 优先选择指定了平台的
            List<OptimizerModel> specModels = modelList.stream().filter(optimizerModel ->
                    !CollectionUtils.isEmpty(optimizerModel.allowedPlatforms)).collect(Collectors.toList());
            // 如果有指定了平台的，就从指定平台中选择一个
            OptimizerModel optimizerModel;
            if (!CollectionUtils.isEmpty(specModels)) {
                optimizerModel = loadBalanceOptimizer(specModels);
            } else {
                // 否则，从所有的中选择一个
                optimizerModel = loadBalanceOptimizer(modelList);
            }
            OptimizerPluginClient pluginClient = new OptimizerPluginClient(optimizerModel);
            try {
                pluginClient.open();
                plan = pluginClient.getClient().optimize(plan, platforms);
            } finally {
                pluginClient.close();
            }
        }
        // channel enrich
        Plan enrichedPlan = channelEnrich.enrichPlan(plan);
        // operator fusion
        return operatorFusion.fusion(enrichedPlan);
    }


    // 使用不同的负载均衡策略来执行优化器，这里简单使用轮训的策略
    private OptimizerModel loadBalanceOptimizer(List<OptimizerModel> optimizerModels) {
        OptimizerModel optimizerModel = optimizerRepository.getLeastFreq(optimizerModels);
        optimizerRepository.addCallFreq(optimizerModel);
        return optimizerModel;
    }

}
