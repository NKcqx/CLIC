package basic.Visitors;

import basic.Operators.Operator;
import basic.PlanTraversal;

public class ExecuteVisitor extends Visitor {
    public ExecuteVisitor(PlanTraversal planTraversal){
        super(planTraversal);
    }

    @Override
    public void visit(Operator opt) {
        opt.evaluate();
    }

}
