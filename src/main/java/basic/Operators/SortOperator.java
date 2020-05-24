package basic.Operators;

import basic.Visitors.Visitor;

public class SortOperator extends Operator{
    // 抽象运算符在全局范围内的唯一标识符
    public final String ID = "SortOperator";
    public SortOperator(String optName) {
        super(optName);
    }

    @Override
    public String getID(){return this.ID;}

    @Override
    public void acceptVisitor(Visitor visitor) {
        visitor.visit(this);
    }

}
