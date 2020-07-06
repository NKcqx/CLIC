/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/7/6 1:40 下午
 */
package basic.visitors;

import basic.operators.Operator;
import basic.traversal.AbstractTraversal;

public abstract class Visitor {
    protected AbstractTraversal planTraversal;

    public Visitor(AbstractTraversal planTraversal) {
        this.planTraversal = planTraversal;
    }

    public abstract void visit(Operator opt);

    public void startVisit() {
        if (planTraversal.hasNextOpt()) {
            Operator opt = planTraversal.nextOpt();
            opt.acceptVisitor(this);
        }

    }

}
