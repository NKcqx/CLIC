package basic.visitors;


import basic.operators.Operator;
import basic.traversal.AbstractTraversal;

import java.util.ArrayList;
import java.util.List;

/**
 * 将所有的operator放在一起，便于argo的运行
 *
 * @author 唐志伟
 * @since 2020/7/6 2:00 PM
 * @version 1.0
 */
public class PipelineVisitor extends Visitor {

    private List<Operator> allOperators = new ArrayList<>();

    public PipelineVisitor(AbstractTraversal planTraversal) {
        super(planTraversal);
    }

    /**
     * 获取所有的operator
     *
     * @return 所有的operator列表
     */
    public List<Operator> getAllOperators() {
        return allOperators;
    }

    @Override
    public void visit(Operator opt) {
        if (!isVisited(opt)) {
            this.allOperators.add(opt);
        }

        if (planTraversal.hasNextOpt()) {
            planTraversal.nextOpt().acceptVisitor(this);
        }
    }

    private boolean isVisited(Operator opt) {
        return this.allOperators.contains(opt);
    }

}
