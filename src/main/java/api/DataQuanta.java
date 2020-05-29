package api;

import basic.Operators.Operator;
import basic.Operators.OperatorMapping;

import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * 相当于DAG图中的每个节点，节点内部保存实际的Operator，提供一系列API以供构造下一跳（可以有多个）
 */
public class DataQuanta {
    private Operator operator;

    public DataQuanta(Operator operator){
        this.operator = operator;
    }

    /**
     * 将API的map接口转为构建Plan时需要的Operator
     * @param udf 实际应该是Function 而不是String，代表map中要执行的操作
     * @param name 本次map操作的名字
     * @return PlanBuilder，这样就可以用pipeline的形式使用API了
     */
    public DataQuanta map(Supplier udf, String name) throws Exception {
        // TODO: UDF怎么传进去 since 系统里没有MapOperator的概念了，但是不同Operator的参数不同，必须得区分
        return this.createDataQuanta("map");
    }

    public DataQuanta sort() throws Exception {
        return this.createDataQuanta("sort");
    }

    public DataQuanta filter() throws Exception {
        return this.createDataQuanta("filter");
    }

    public DataQuanta collect() throws Exception {
        return this.createDataQuanta("collect");
    }

    //
    public int acceptIncoming(DataQuanta incoming){
        this.operator.connectFrom(incoming.getOperator());
        return this.operator.getIncoming_opt().size();
    }

    public Operator getOperator() {
        return operator;
    }

    private DataQuanta createDataQuanta(String operator_ability) throws Exception {
        Operator opt = OperatorMapping.createOperator(operator_ability);
        this.operator.connectTo(opt);
        return new DataQuanta(opt);
    }

}
