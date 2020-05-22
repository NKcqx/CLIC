package platforms.Spark.Operators;

import basic.Operators.ExecutableOperator;
import basic.Operators.MapOperator;
import basic.Operators.SortOperator;

public class SparkSortOperator extends SortOperator implements ExecutableOperator {
    public SparkSortOperator( String optName) {
        super( optName);
    }
    public SparkSortOperator(SortOperator opt){super(opt.getOptName());}

    @Override
    public void evaluate(String input, String output) {
        System.out.println(">>  "  + this.toString() + String.format( ".evaluate(%s, %s)", input, output));
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()+"["+this.hashCode()+"]";
    }

    @Override
    public Double getCost() {
        return 13.762; // 临时自定义，理应动态的分析数据量
    }
}
