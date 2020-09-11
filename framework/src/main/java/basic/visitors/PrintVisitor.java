
package basic.visitors;

import basic.operators.Operator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/7/6 1:40 下午
 */
public class PrintVisitor extends Visitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(PrintVisitor.class);
    private List<Operator> visited = new ArrayList<>();

    public PrintVisitor() {
        super();
    }

    @Override
    public void visit(Operator opt) {
        if (!isVisited(opt)) {
            this.logging(opt.toString());
            this.visited.add(opt);
        }
//
//        if (planTraversal.hasNextOpt()) {
//            planTraversal.nextOpt().acceptVisitor(this);
//        }
    }

    private boolean isVisited(Operator opt) {
        return this.visited.contains(opt);
    }

    private void logging(String s) {
        LOGGER.info(s);
    }
}
