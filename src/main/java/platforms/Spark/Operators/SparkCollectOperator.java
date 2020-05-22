package platforms.Spark.Operators;

import basic.Operators.CollectOperator;
import basic.Operators.ExecutableOperator;

public class SparkCollectOperator extends CollectOperator implements ExecutableOperator {

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
    public Double getCost() {
        return 7.1138; // 临时自定义，理应动态的分析数据量
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()+"["+this.hashCode()+"]";
    }
}
