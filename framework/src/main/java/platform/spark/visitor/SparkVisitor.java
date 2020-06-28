package platform.spark.visitor;

import org.apache.spark.sql.SparkSession;

public interface SparkVisitor {
    <T> T execute(SparkSession sparkSession);

}
