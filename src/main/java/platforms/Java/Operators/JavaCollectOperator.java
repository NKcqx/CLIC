package platforms.Java.Operators;

import basic.Operators.CollectOperator;
import basic.Operators.ExecutableOperator;
import basic.Operators.Visitable;
import basic.Visitors.Visitor;

public class JavaCollectOperator extends CollectOperator implements ExecutableOperator, Visitable {
    public JavaCollectOperator() {
        super("DefaultJavaCollectOperator");
    }

    public JavaCollectOperator(String optName) {
        super(optName);
    }

    public JavaCollectOperator(CollectOperator opt){
        super(opt.getOptName());
    }

    /**
     * 克隆构造函数
     * @param opt 被克隆实体
     */
    public JavaCollectOperator(JavaCollectOperator opt){
        super(opt.getOptName());
    }



    @Override
    public void evaluate(String input, String output) {
        System.out.println(">>  "  + this.toString());
    }

    @Override
    public Double getCost() {
        return 13.6521; // 临时自定义，理应动态的分析数据量
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "["+this.hashCode()+"]";
    }

    @Override
    public void acceptVisitor(Visitor visitor) {
        visitor.visit(this);
    }
}
