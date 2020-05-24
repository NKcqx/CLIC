package platforms.Spark.Operators;

import basic.Operators.CollectOperator;
import basic.Operators.ExecutableOperator;
import basic.Operators.Visitable;
import basic.Visitors.Visitor;

public class SparkCollectOperator extends CollectOperator implements ExecutableOperator, Visitable {

    public SparkCollectOperator(String optName) {
        super(optName);
    }

    public SparkCollectOperator(CollectOperator opt){
        super(opt.getOptName());
    }

    @Override
    public void evaluate(String input, String output) {
        System.out.println(">>  "  + this.toString() + String.format( ".evaluate(%s, %s)", input, output));
    }

    @Override
    public void acceptVisitor(Visitor visitor) {
        visitor.visit(this);
    }

    @Override
    public Double getCost() {
        return 7.1138; // 临时自定义，理应动态的分析数据量
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()+"["+this.hashCode()+"]";
    }
}
