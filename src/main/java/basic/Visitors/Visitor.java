package basic.Visitors;

import basic.Operators.Operator;
import basic.PlanTraversal;

public abstract class Visitor {
    protected PlanTraversal planTraversal;
    public Visitor(PlanTraversal planTraversal){
        this.planTraversal = planTraversal;
    }

    public abstract void visit(Operator opt);

    public void startVisit() {
        if (planTraversal.hasNextOpt())
            planTraversal.nextOpt().acceptVisitor(this);
    }
}
