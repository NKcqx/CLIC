package platform.spark;

import api.PlanBuilder;
import basic.operators.Operator;
import org.apache.spark.sql.SparkSession;
import platform.spark.visitor.SparkVisitor;
import platform.spark.visitor.SparkVisitorConfiguration;

import java.lang.reflect.Method;

public final class SparkPlatform {
    private static SparkPlatform singleton;
    // 静态变量保存SparkSession, 单例模式，保证sparkSession仅仅初始化一次
    private SparkSession sparkSession;

    private SparkPlatform() {
        sparkSession = SparkSession.builder()
                .master("local")
                .appName("JobX").getOrCreate();
        this.sparkSession = sparkSession;
    }

    private static synchronized SparkPlatform getSingleton() {
        if (singleton == null) {
            singleton = new SparkPlatform();
            return singleton;
        } else return singleton;
    }

    public static Object SparkRunner(PlanBuilder planBuilder) {
        SparkPlatform sparkPlatform = getSingleton();
        Operator tail = sparkPlatform.getSinkOperator(planBuilder);
        return sparkPlatform.Visitor(tail);
    }

    public static SparkVisitor convertOperator2SparkVisitor(Operator operator) {
        SparkVisitor result;
        String operatorName = operator.getOperatorName();
        Class<?> visitorClass = SparkVisitorConfiguration.opMap.get(operatorName);
        try {
            Method newInstanceMethod = visitorClass.getMethod("newInstance", Operator.class);
            result = (SparkVisitor) newInstanceMethod.invoke(null, operator);
        } catch (Exception e) {
            e.printStackTrace();
            throw new AssertionError("operator " + operator.getOperatorName() + " not exists:\n" + e.getMessage());
        }
        return result;
    }

    private Object Visitor(Operator tail) {
        return convertOperator2SparkVisitor(tail).execute(sparkSession);
    }

    private Operator getSinkOperator(PlanBuilder planBuilder) {
        Operator traverse = planBuilder.getHeadDataQuanta().getOperator();
        while (traverse.getOutputChannel().size() != 0) {
            // TODO: 假设DAG只有一个sink
            traverse = traverse.getOutputChannel().get(0).getTargetOperator();
        }
        return traverse;
    }
}
