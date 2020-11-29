package siamese;

import basic.operators.Operator;
import basic.operators.OperatorFactory;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

/**
 * 工厂类，用于根据Siamese返回的树的节点创建CLIC的逻辑算子
 *
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/11/29 1:53 PM
 */
public class SiameseOptFactory {

    // table名与table地址的映射表
    public static Map<String, String> tableAddrMap = SiameseSchema.getTableAddrMap();

    /**
     * LogicalPlan节点所带的udf参数还需要先处理一下再写进yaml文件，再传给物理平台的DataFrame API
     * @param condition
     * @return
     */
    private static String processConditionStr(String condition, String ability) {
        // 处理"&&"子串
        if (condition.contains("&&")) {
            condition = condition.replaceAll("&&", "and");
        }
        // 处理"#xxx"的句柄
        if (condition.matches(".*#\\d+.*")) {
            condition = condition.replaceAll("#\\d+", "");
        }
        if (ability.equals("t-join")) {
            // 比如，Siamese给CLIC返回的join节点的condition为"Some((id = id))"
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
            System.out.println(condition);
        }
        return condition;
    }

    /**
     * 根据树节点的schema获取字段名称
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
    public static Operator createTProject(LogicalPlan node) throws Exception {
        String ability = "t-project";
        String fieldNames = getFieldNames(node.schema());

        Operator opt = OperatorFactory.createOperator(ability);
        opt.setParamValue("schema", node.schema().toString());
        opt.setParamValue("condition", fieldNames);
        return opt;
    }
}
