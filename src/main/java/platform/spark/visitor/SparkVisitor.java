package platform.spark.visitor;

import basic.Operators.Operator;
import org.apache.spark.sql.SparkSession;

import java.lang.reflect.Method;

public interface SparkVisitor {
    <T> T execute(SparkSession sparkSession);

}
