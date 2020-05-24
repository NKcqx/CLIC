package basic.Visitors;

import basic.Operators.ExecutableOperator;
import basic.Operators.Operator;

public interface Visitor {
    void visit(Operator opt);
    void visit(ExecutableOperator opt);
}
