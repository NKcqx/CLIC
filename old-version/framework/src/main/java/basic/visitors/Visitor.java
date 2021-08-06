
package basic.visitors;

import basic.operators.Operator;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/7/6 1:40 下午
 */
public abstract class Visitor {

    public Visitor() {

    }

    public abstract void visit(Operator opt);

}
