package basic.Visitors;


import basic.Operators.Operator;
import basic.traversal.AbstractTraversal;

import java.util.ArrayList;
import java.util.List;

/**
 * 将所有的operator放在一起，便于argo的运行
 */
public class PipelineVisitor extends Visitor {

    public PipelineVisitor(AbstractTraversal planTraversal) {
        super(planTraversal);
    }

    private List<Operator> allOperators = new ArrayList<>();

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
        if (!isVisited(opt)){
            this.allOperators.add(opt);
        }

        if (planTraversal.hasNextOpt()){
            planTraversal.nextOpt().acceptVisitor(this);
        }
    }

    private boolean isVisited(Operator opt){
        return this.allOperators.contains(opt);
    }

}
