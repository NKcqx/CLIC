package basic.Visitors;

import basic.Operators.Operator;
import basic.traversal.AbstractTraversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ExecuteVisitor extends Visitor {
    private static final Logger logger = LoggerFactory.getLogger(ExecuteVisitor.class);
    public ExecuteVisitor(AbstractTraversal planTraversal){
        super(planTraversal);
    }
    private List<Operator> visited = new ArrayList<>();

    @Override
    public void visit(Operator opt) {
        boolean successfully_execute = false;
        if (!isVisited(opt)){
            successfully_execute = opt.evaluate();
            if (successfully_execute)
                this.visited.add(opt);
        }
        if (planTraversal.hasNextOpt()){ //TODO: 这有BUG，对未成功执行的opt没有起到限制不做addSuccessor的作用
            Operator nextOpt = planTraversal.nextOpt();
            planTraversal.addSuccessor(nextOpt);
            nextOpt.acceptVisitor(this);
        }

    }

    private boolean isVisited(Operator opt){
        return this.visited.contains(opt);
    }
}
