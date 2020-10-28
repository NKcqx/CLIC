package fdu.daslab.executable.sparksql.utils;

import org.apache.spark.sql.SparkSession;

/**
 * 初始化spark
 *
 * @author 刘丰艺
 * @since 2020/10/27 5:30 PM
 * @version 1.0
 */
public class SparkInitUtil {
    private static SparkSession sparkSession = SparkSession.builder()
            .master("local").appName("SparkSQLStage").getOrCreate();

    public static SparkSession getDefaultSparkSession() {
        return sparkSession;
    }
}
