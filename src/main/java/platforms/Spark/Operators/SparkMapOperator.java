package platforms.Spark.Operators;

import basic.Operators.ExecutableOperator;
import basic.Operators.MapOperator;

import java.util.function.Function;
import java.util.function.Supplier;

public class SparkMapOperator extends MapOperator implements ExecutableOperator {
    public SparkMapOperator(Supplier udf, String optName) {
        super(udf, optName);
    }

    public SparkMapOperator(MapOperator opt){
        super(opt.getUDF(), opt.getOptName());
    }

    @Override
    public void evaluate(String input, String output) {
        this.func.get();
        System.out.println(">>  "  + this.toString());
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() +"["+this.hashCode()+"]";
    }

    @Override
    public Double getCost() {
        return 27.9391; // 临时自定义，理应动态的分析数据量
    }
}
