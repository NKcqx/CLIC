package platforms.Java.Operators;

import basic.Operators.ExecutableOperator;
import basic.Operators.SortOperator;
import basic.Operators.Visitable;
import basic.Visitors.Visitor;

public class JavaSortOperator extends SortOperator implements ExecutableOperator, Visitable {
    public JavaSortOperator(String optName) {
        super( optName);
    }
    public JavaSortOperator(SortOperator opt){
        super(opt.getOptName());
    }

    @Override
    public void evaluate(String input, String output) {
        System.out.println(">>  "  + this.toString() + String.format( ".evaluate(%s, %s)", input, output));
    }

    @Override
    public Double getCost() {
        return 2.58; // 临时自定义，理应动态的分析数据量
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()+"["+this.hashCode()+"]";
    }

    @Override
    public void acceptVisitor(Visitor visitor) {
        visitor.visit((ExecutableOperator) this);
    }
}
