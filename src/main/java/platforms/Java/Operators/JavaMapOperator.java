package platforms.Java.Operators;

import basic.Operators.ExecutableOperator;
import basic.Operators.MapOperator;

import java.util.function.Function;
import java.util.function.Supplier;

public class JavaMapOperator extends MapOperator implements ExecutableOperator {
    public JavaMapOperator(Supplier udf, String optName) {
        super(udf, optName);
    }

    public JavaMapOperator(MapOperator opt){
        super(opt.getUDF(), opt.getOptName());
    }

    @Override
    public void evaluate(String input, String output) {
        this.func.get();
        System.out.println(">>  "  + this.toString() + "Get Input:  " );
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "["+this.hashCode()+"]";
    }

    @Override
    public Double getCost() {
        return 32.9963; // 临时自定义，理应动态的分析数据量
    }
}
