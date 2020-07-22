package optimizer.rules;

import basic.operators.Operator;

/**
 * 优化规则，优化的对象是一个DAG，在我们系统中使用 operator + channel描述
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/7/21 4:06 PM
 */
public abstract class Rule {

    // 规则适用的次数，默认对一个DAG可以执行无数次
    private boolean once = false;

    /**
     * 判断dag是否能够匹配上这个rule
     *
     * @param plan dag
     * @return 是否
     */
    public abstract boolean isMatched(Operator plan);

    /**
     * 对DAG优化，返回优化后的DAG
     *
     * @param plan dag的头节点
     * @return 新的dag
     */
    public abstract Operator apply(Operator plan);

    public boolean isOnce() {
        return once;
    }

    public void setOnce(boolean once) {
        this.once = once;
    }
}
