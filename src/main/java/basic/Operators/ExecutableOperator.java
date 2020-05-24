package basic.Operators;

/***
 * 给Execution Operator用的，basic的Operator是抽象的 无法Execute，因此不能实现这个接口
 */
public interface ExecutableOperator extends Visitable {
    Double cardinality = 0.;

    /**
     * 所有Execution Plan中的Opt.在运行时都是调用这个接口进行实际计算
     * @param input Opt.的输入数据
     * @param output 输出
     */
    void evaluate(String input, String output);

    /**
     * @return Opt.实际运算时的性能
     */
    Double getCost();
}
