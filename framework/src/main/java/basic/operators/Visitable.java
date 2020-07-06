
package basic.operators;

import basic.visitors.Visitor;

/**
 * @author 陈齐翔
 * @version 1.0
 * @since 2020/7/6 1:40 下午
 */
public interface Visitable {
    void acceptVisitor(Visitor visitor);
}
