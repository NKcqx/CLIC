
package basic.traversal;

import basic.operators.Operator;

public abstract class AbstractTraversal {

    private final Operator root;

    public AbstractTraversal(Operator root) {
        this.root = root;
    }

    public abstract boolean hasNextOpt();

    public abstract Operator nextOpt();

    //todo unnessasery
    public abstract int addSuccessor(Operator operator);
}
