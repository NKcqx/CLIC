package basic.operators;

import basic.visitors.Visitor;

public interface Visitable {
    void acceptVisitor(Visitor visitor);
}
