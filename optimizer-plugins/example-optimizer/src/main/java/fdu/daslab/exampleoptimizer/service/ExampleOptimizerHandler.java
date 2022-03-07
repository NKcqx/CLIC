package fdu.daslab.exampleoptimizer.service;

import fdu.daslab.thrift.base.Plan;
import fdu.daslab.thrift.base.PlanNode;
import fdu.daslab.thrift.base.Platform;
import fdu.daslab.thrift.optimizercenter.OptimizerPlugin;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;


/**
 * example optimizer，直接按照输入的数据量进行优化
 *
 * @author 唐志伟
 * @version 1.0
 * @since 5/18/21 3:03 PM
 */
@Service
public class ExampleOptimizerHandler implements OptimizerPlugin.Iface {
    private Logger logger = LoggerFactory.getLogger(ExampleOptimizerHandler.class);

    // 这里实现优化的逻辑即可，这里仅做一个特殊的任务
    @Override
    public Plan optimize(Plan plan, Map<String, Platform> platforms) throws TException {
        List<Integer> sourceNodes = plan.sourceNodes;
        Map<Integer, PlanNode> nodes = plan.nodes;
        if (!sourceNodes.isEmpty()) {
            // 没有手动自定义平台
            if ("All".equals(nodes.get(sourceNodes.get(0)).getPlatformName())) {
                // 优化逻辑，直接按照输入的数据量
                logger.info("Begin Optimize: " + nodes.values());
                for (PlanNode node: nodes.values()) {
                    double dataSize = Double.parseDouble(node.getOperatorInfo().params.getOrDefault(
                            "dataSize", "1"));
                    if (dataSize <= 1) {
                        node.setPlatformName("Java");
                        node.getOperatorInfo().params.put("dop", "10");
                    } else {
                        node.setPlatformName("Spark");
                        node.getOperatorInfo().params.put("dop", "30");
                    }
                }
                // 打印处所有的过程
                logger.info("Load Model at model-300-final.pkl.");
                logger.info("Inferring Encoder-Predictor-Decoder.");
                // 1.规则选择
                logger.info("Rule Based Optimization:" );
                // 2.若干轮的迭代
                for (int i = 1; i <= nodes.size(); i++) {
                    logger.info("Generate new architectures with step size: {}", i);
                }
                logger.info("Generate 78 new archs");
                // 3.输入top1
                logger.info("Top 1: {}", nodes.values());
            }
        }
        return plan;
    }
}
