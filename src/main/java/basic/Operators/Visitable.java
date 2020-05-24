package basic.Operators;

import basic.Visitors.Visitor;

public interface Visitable {
    abstract void acceptVisitor(Visitor visitor);
}
