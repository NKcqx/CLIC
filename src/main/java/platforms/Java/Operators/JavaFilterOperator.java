package platforms.Java.Operators;

import basic.Operators.FilterOperator;
import basic.Operators.ExecutableOperator;

import java.util.function.Predicate;

public class JavaFilterOperator extends FilterOperator implements ExecutableOperator {
    public JavaFilterOperator(Predicate predicate, String optName) {
        super(predicate, optName);
    }

    public JavaFilterOperator(FilterOperator opt){
        super(opt.getUDF(), opt.getOptName());
    }

    @Override
    public void evaluate(String input, String output) {
        System.out.println(">>  "  + this.toString());
    }

    @Override
    public Double getCost() {
        return 21.4324; // 临时自定义，理应动态的分析数据量
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "["+this.hashCode()+"]";
    }
}
