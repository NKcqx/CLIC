package basic.Visitors;

import basic.Operators.ExecutableOperator;
import basic.Operators.Operator;

public class ExecuteVisitor implements Visitor {
    @Override
    public void visit(ExecutableOperator opt) {
        opt.evaluate("input", "output");
    }

    @Override
    public void visit(Operator opt) {
        // do nothing
    }

}
