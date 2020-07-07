
package basic.visitors;

import basic.operators.Operator;
import basic.traversal.AbstractTraversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/7/6 1:40 下午
 */
public class ExecuteVisitor extends Visitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExecuteVisitor.class);
    private List<Operator> visited = new ArrayList<>();

    public ExecuteVisitor(AbstractTraversal planTraversal) {
        super(planTraversal);
    }

    @Override
    public void visit(Operator opt) {
        boolean successfullyExecute = false;
        if (!isVisited(opt)) {
            successfullyExecute = opt.evaluate();
            if (successfullyExecute) {
                this.visited.add(opt);
            }
        }
        if (planTraversal.hasNextOpt()) { //TODO: 这有BUG，对未成功执行的opt没有起到限制不做addSuccessor的作用
            Operator nextOpt = planTraversal.nextOpt();
            planTraversal.addSuccessor(nextOpt);
            nextOpt.acceptVisitor(this);
        }

    }

    private boolean isVisited(Operator opt) {
        return this.visited.contains(opt);
    }
}
