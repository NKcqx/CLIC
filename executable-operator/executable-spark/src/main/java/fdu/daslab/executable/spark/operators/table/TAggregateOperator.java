package fdu.daslab.executable.spark.operators.table;

import fdu.daslab.executable.basic.model.OperatorBase;
import fdu.daslab.executable.basic.model.ParamsModel;
import fdu.daslab.executable.basic.model.ResultModel;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * table的aggregate算子
 *
 * @author 刘丰艺
 * @since 2020/12/2 7:30 PM
 * @version 1.0
 */
public class TAggregateOperator extends OperatorBase<Dataset<Row>, Dataset<Row>> {
    public TAggregateOperator(String id, List<String> inputKeys, List<String> outputKeys, Map<String, String> params) {
        super("SparkTAggregateOperator", id, inputKeys, outputKeys, params);
    }

    @Override
    public void execute(ParamsModel inputArgs, ResultModel<Dataset<Row>> result) {
        Dataset<Row> df = this.getInputData("data");
        // 提取聚合函数的udf
        if (this.params.get("aggregateCondition") == null) {
            throw new IllegalArgumentException("聚合算子选择的列属性不能为空！");
        }
        String[] aggregateCondition = this.params.get("aggregateCondition").split(",");
        Map<String, String> aggMap = new HashMap<>();
        for (String aggFuncAndCol : aggregateCondition) {
            String[] colFuncPair = aggFuncAndCol.split("-");
            if (colFuncPair.length != 2) {
                throw new IllegalArgumentException("聚合函数与列属性的匹配格式不对");
            }
            aggMap.put(colFuncPair[0], colFuncPair[1]);
        }

        // 开始聚合操作
        // 根据group by参数的数目分类讨论
        if (this.params.get("groupCondition") == null) {
            // 如果没有group by参数，直接进行聚合
            df = df.agg(aggMap);
        } else {
            String[] groupCondition = this.params.get("groupCondition").split(",");
            Column[] cols = new Column[groupCondition.length];
            for (int i = 0; i < groupCondition.length; i++) {
                Column col = new Column(groupCondition[i]);
                cols[i] = col;
            }
            df = df.groupBy(cols).agg(aggMap);
        }
        this.setOutputData("result", df);
    }
}
