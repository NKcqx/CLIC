package optimizer;

import basic.operators.Operator;
import channel.Channel;
import optimizer.rules.FilterPushDown;
import optimizer.rules.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 * 基于规则的优化器，由于优化涉及到UDF，具有一定复杂性，因此直接将优化的决定权给用户
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/7/21 3:24 PM
 */
public class Optimizer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Optimizer.class);

    /**
     * 目前支持的规则，规则默认是有序的
     */
    private final Map<String, Rule> batches = new LinkedHashMap<String, Rule>() {{
        put("filterPushDown", new FilterPushDown());
    }};

    /**
     * 对DAG进行优化，channel暂时不考虑，原因是优化后channel不知道如何变化
     *
     * @param plan 头节点
     * @return 新的dag
     */
    public Operator execute(Operator plan) {
        // 遍历所有的规则，执行优化
        Operator parentOperator = new Operator();
        SwitchOperatorUtil.connectTwoOperator(parentOperator, plan);
        batches.forEach((ruleName, rule) -> {
            // 遍历每一个operator，匹配，应用规则
            Operator parentPlan = parentOperator;
            Operator curPlan = parentPlan.getOutputChannel().get(0).getTargetOperator();
            Queue<Operator> operatorQueue = new LinkedList<>();
            operatorQueue.add(curPlan);
            while (!operatorQueue.isEmpty()) {
                curPlan = operatorQueue.poll();
                if (rule.isMatched(curPlan)) {
                    LOGGER.info("Rule " + ruleName + " hit: curPlan=" + curPlan.getOperatorID());
                    Operator newPlan = rule.apply(curPlan);
                    SwitchOperatorUtil.connectTwoOperatorAlone(parentPlan, newPlan);
                    if (rule.isOnce()) {
                        // 该规则只执行一次
                        break;
                    }
                    // 对优化后的子DAG继续执行规则
                    operatorQueue.add(newPlan);
                } else {
                    parentPlan = curPlan;
                    for (Channel nextChannel : curPlan.getOutputChannel()) {
                        operatorQueue.add(nextChannel.getTargetOperator());
                    }
                }

            }
        });
        return parentOperator.getOutputChannel().get(0).getTargetOperator();
    }

}
