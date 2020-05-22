package platforms.Java.Operators;

import basic.Operators.CollectOperator;
import basic.Operators.ExecutableOperator;

public class JavaCollectOperator extends CollectOperator implements ExecutableOperator {
    public JavaCollectOperator(String optName) {
        super(optName);
    }

    public JavaCollectOperator(CollectOperator opt){
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
}
