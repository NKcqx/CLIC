package basic.Visitors;

import basic.Operators.Operator;
import basic.PlanTraversal;

import java.util.ArrayList;
import java.util.List;

public class ExecuteVisitor extends Visitor {
    public ExecuteVisitor(PlanTraversal planTraversal){
        super(planTraversal);
    }
    private List<Operator> visited = new ArrayList<>();

    @Override
    public void visit(Operator opt) {
        if (!isVisited(opt)){
            // TODO: 得拿到顺着哪个Channel下来的？
            opt.evaluate(0);
            this.visited.add(opt);
        }
        if (planTraversal.hasNextOpt())
            planTraversal.nextOpt().acceptVisitor(this);

    }
    private boolean isVisited(Operator opt){
        return this.visited.contains(opt);
    }
}
