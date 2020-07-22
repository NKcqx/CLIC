package optimizer.rules;

import basic.operators.Operator;
import channel.Channel;
import optimizer.SwitchOperatorUtil;

import java.util.List;

/**
 * @author 唐志伟
 * @version 1.0
 * @since 2020/7/21 4:32 PM
 */
public class FilterPushDown extends Rule {

    @Override
    public boolean isMatched(Operator plan) {
        // 匹配条件：filter和sort算子都是deterministic；filter和map都是deterministic
        // 只考虑单个输出
        if (plan != null && plan.getOutputChannel().size() == 1) {
            Operator child = plan.getOutputChannel().get(0).getTargetOperator();
            return ("MapOperator".equals(plan.getOperatorName()) && plan.isDeterministic()
                    || "SortOperator".equals(plan.getOperatorName()) && plan.isDeterministic())
                    && "FilterOperator".equals(child.getOperatorName()) && child.isDeterministic();
        }
        return false;
    }

    @Override
    public Operator apply(Operator plan) {
        // 将filter算子放到前面
        Operator filterOpt = plan.getOutputChannel().get(0).getTargetOperator();
        List<Channel> nextChannel = filterOpt.getOutputChannel();
        plan.cleanOutputChannel();
        nextChannel.forEach(channel -> {
            Operator target = channel.getTargetOperator();
            target.cleanInputChannel();
            SwitchOperatorUtil.connectTwoOperator(plan, target);
        });
        SwitchOperatorUtil.connectTwoOperatorAlone(filterOpt, plan);
        return filterOpt;
    }
}
