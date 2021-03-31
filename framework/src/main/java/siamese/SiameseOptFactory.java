package siamese;

import basic.operators.Operator;
import basic.operators.OperatorFactory;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.*;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;

/**
 * 工厂类，用于根据Siamese返回的树的节点创建CLIC的逻辑算子
 *
 * Siamese还没有封装好他们的包给我们用，所以先用真正的Spark SQL代替
 * 逻辑阶段与物理阶段混淆，这是对接的锅，没办法
 *
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/11/29 1:53 PM
 */
public class SiameseOptFactory {

    // table名与table地址的映射表
    public static Map<String, String> tableAddrMap = SiameseSchema.getTableAddrMap();

    public static Operator createOperator(LogicalPlan node) throws Exception {
        Operator opt = null;
        if (node.getClass().equals(LogicalRelation.class)) {
            opt = createTRelationOpt(node);
        }
        if (node.getClass().equals(Filter.class)) {
            opt = createTFilterOpt(node);
        }
        if (node.getClass().equals(Join.class)) {
            opt = createTJoinOpt(node);
        }
        if (node.getClass().equals(Project.class)) {
            opt = createTProjectOpt(node);
        }
        if (node.getClass().equals(Aggregate.class)) {
            opt = createTAggregateOpt(node);
        }
        return opt;
    }

    /**
     * 对多数LogicalPlan节点来说
     * 其所带的udf参数还需要先处理一下，再写进yaml文件
     * 再传给物理平台的DataFrame API使用
     * @param condition 原udf语句
     * @param ability 有时候需要对特定类型的算子进行特定处理
     * @return
     */
    private static String processConditionStr(String condition, String ability) {
        // 如果有"&&"，则换成"and"
        if (condition.contains("&&")) {
            condition = condition.replaceAll("&&", "and");
        }
        // 如果有"#xxx"的句柄，则删除
        if (condition.matches(".*#\\d+.*")) {
            condition = condition.replaceAll("#\\d+", "");
        }
        // 如果字符串首尾两端有'['和']'，则删除
        if (condition.matches("\\[.*\\]")) {
            condition = condition.substring(1, condition.length() - 1);
        }
        // 如果SQL中含有"having"字句，则最后最顶层的节点一定是一个udf带有聚合函数的project节点
        // 比如这个project节点的udf为"sum(CAST(grade AS DOUBLE)), avg(CAST(sgrade AS DOUBLE))"
        // 对于这个project节点，无需执行它
        // 因为它的上一个节点的输出结果的列属性名就已经带有聚合函数声明，比如"sum(grade)"
        // 如果到这个project节点还继续以"select sum(grade) from ..."字样作为udf来执行的话，是违反语法的
        // 因为列属性名已经变成了"sum(grade)"，而不是"grade"
        if (ability.equals("t-project")) {
            if (condition.contains("CAST") || condition.contains("cast")) {
                condition = "no need to exe";
            }
        }
        // 对join节点，它的udf需要特别处理
        if (ability.equals("t-join")) {
            // 例如Siamese给CLIC返回的join节点的condition为"Some((id = id))"
            // 那么CLIC需要将"id"提取出来
            int equalSignIndex = condition.indexOf(" = ");
            int bracketsIndex = condition.indexOf("(");
            int i = 0;
            // 保险起见，先假设括号深度为5
            while (bracketsIndex < equalSignIndex && i < 5) {
                // 寻找下一个左括号
                if (condition.indexOf("(", bracketsIndex + 1) != -1) {
                    bracketsIndex = condition.indexOf("(", bracketsIndex + 1);
                }
                i++;
            }
            condition = condition.substring(bracketsIndex+1, equalSignIndex);
        }
        // 对aggregate节点，它有group by的udf以及aggregate的udf
        // 其中的aggregate udf需要特别处理
        if (ability.equals("t-aggregate") && condition.matches(".*\\(.*")) {
            // 例如"avg(CAST(grade AS DOUBLE))"
            // 需要让aggCol = "grade"，让aggFunc = "avg"
            String aggFunc = condition.substring(0, condition.indexOf("("));
            // TODO: 从csv中读取出来的数字都是string类型？
            //  现在对要聚合操作的数据，Spark SQL都会先cast成double等数字型类型
            //  如果数据源是Database，需要补充考虑
            String aggCol = "";
            if (condition.contains("CAST")) {
                aggCol = condition.substring(condition.indexOf("CAST(") + 5, condition.indexOf(" AS "));
            }
            if (condition.contains("cast")) {
                aggCol = condition.substring(condition.indexOf("cast(") + 5, condition.indexOf(" as "));
            }
            condition = aggCol + "-" + aggFunc;
        }
        return condition;
    }

    /**
     * 根据树节点的schema获取字段名称
     * 暂无用途
     * @param schema
     * @return
     */
    private static String getFieldNames(StructType schema) {
        StructType structType = schema;
        StructField[] structFields = structType.fields();
        StringBuilder fieldNames = new StringBuilder();
        for (int i = 0; i<structFields.length; i++) {
            fieldNames.append(structFields[i].name());
            fieldNames.append(",");
        }
        fieldNames.deleteCharAt(fieldNames.length() - 1);
        return fieldNames.toString();
    }

    /**
     * 创建一个t-filter类型的Operator，并设置相关参数
     * @param node
     * @return
     */
    public static Operator createTFilterOpt(LogicalPlan node) throws Exception {
        String ability = "t-filter";
        String condition = ((Filter) node).condition().toString();
        condition = processConditionStr(condition, ability);

        Operator opt = OperatorFactory.createOperator(ability);
        opt.setParamValue("schema", node.schema().toString());
        opt.setParamValue("condition", condition);
        return opt;
    }

    /**
     * 创建一个t-relation类型的Operator，并设置相关参数
     * @param node
     * @return
     */
    public static Operator createTRelationOpt(LogicalPlan node) throws Exception {
        String ability = "t-relation";
        String tableName = SiameseSchema.getTableName(node);

        Operator opt = OperatorFactory.createOperator(ability);
        opt.setParamValue("schema", node.schema().toString());
        opt.setParamValue("tableName", tableName);
        opt.setParamValue("inputPath", tableAddrMap.get(tableName));
        return opt;
    }

    /**
     * 创建一个t-join类型的Operator，并设置相关参数
     * @param node
     * @return
     */
    public static Operator createTJoinOpt(LogicalPlan node) throws Exception {
        String ability = "t-join";
        String condition = ((Join) node).condition().toString();
        condition = processConditionStr(condition, ability);

        Operator opt = OperatorFactory.createOperator(ability);
        opt.setParamValue("schema", node.schema().toString());
        opt.setParamValue("condition", condition);
        return opt;
    }

    /**
     * 创建一个t-project类型的Operator，并设置相关参数
     * @param node
     * @return
     */
    public static Operator createTProjectOpt(LogicalPlan node) throws Exception {
        String ability = "t-project";
        String condition = ((Project) node).argString();
        condition = processConditionStr(condition, ability);

        Operator opt = OperatorFactory.createOperator(ability);
        opt.setParamValue("schema", node.schema().toString());
        opt.setParamValue("condition", condition);
        return opt;
    }

    /**
     * 创建一个t-aggregate类型的Operator，并设置相关参数
     * Spark SQL给DataFrame API主要提供了max、min、sum、avg这几种操作
     * 聚合操作可能带有group by，也可能没有
     * 聚合操作可能带有having，也可能没有
     * 经过Siamese对语法树的解析和优化，有having相比没有having只是在Aggregate节点上多一层Filter和Project节点
     * @param node
     * @return
     */
    public static Operator createTAggregateOpt(LogicalPlan node) throws Exception {
        String ability = "t-aggregate";
        // 聚合函数的udf表达式分为group by和aggregate两种
        // 先获取group by的udf
        List<Expression> grExps = scala.collection.JavaConversions.seqAsJavaList(
                ((Aggregate) node).groupingExpressions()
        );
        StringBuilder groupUdf = new StringBuilder();
        // group by可能会根据多个列属性来分组，因此需要用for循环来获取多个列属性
        for (Expression grExp : grExps) {
            // udf语句还需要先处理一下再写进yaml文件
            // 例如算子的其中一个grExp取值是"gender#12"，需要将句柄"#12"删除
            groupUdf.append(processConditionStr(grExp.toString(), ability));
            groupUdf.append(",");
        }

        // 再获取aggregate的udf
        List<NamedExpression> aggExps = scala.collection.JavaConversions.seqAsJavaList(
                ((Aggregate) node).aggregateExpressions()
        );
        StringBuilder aggregateUdf = new StringBuilder();
        // aggregate与group by同理，聚合操作选取的列属性也会不止一个
        for (NamedExpression aggExp : aggExps) {
            // udf语句还需要先处理一下再写进yaml文件
            // 例如算子的其中一个aggExp.name()取值是"avg(CAST(grade AS DOUBLE))"
            // 需要将功能"avg"和选取的列"grade"提取出来，并拼接成"avg-grade"，表示用户想对"成绩"属性求平均值
            aggregateUdf.append(processConditionStr(aggExp.name(), ability));
            aggregateUdf.append(",");
        }
        // 把最后的逗号删掉
        aggregateUdf.deleteCharAt(aggregateUdf.length() - 1);

        Operator opt = OperatorFactory.createOperator(ability);
        opt.setParamValue("schema", node.schema().toString());
        if (groupUdf.length() > 0) {
            // 把最后的逗号删掉
            groupUdf.deleteCharAt(groupUdf.length() - 1);
            opt.setParamValue("groupCondition", groupUdf.toString());
        } else {
            opt.setParamValue("groupCondition", null);
        }
        opt.setParamValue("aggregateCondition", aggregateUdf.toString());
        return opt;
    }
}
