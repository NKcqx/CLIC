package platforms.Spark.Operators;

import basic.Operators.ExecutableOperator;
import basic.Operators.FilterOperator;

import java.util.function.Predicate;
import java.util.function.Supplier;

public class SparkFilterOperator extends FilterOperator implements ExecutableOperator {
    public SparkFilterOperator(Predicate udf, String optName) {
        super(udf, optName);
    }

    public SparkFilterOperator(FilterOperator opt){
        super(opt.getUDF(), opt.getOptName());
    }

    @Override
    public void evaluate(String input, String output) {
        System.out.println(">>  "  + this.toString());
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() +"["+this.hashCode()+"]";
    }

    @Override
    public Double getCost() {
        return 29.0091; // 临时自定义，理应动态的分析数据量
    }
}
