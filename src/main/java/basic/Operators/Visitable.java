package basic.Operators;

import basic.Visitors.Visitor;

public interface Visitable {
    void acceptVisitor(Visitor visitor);
}
