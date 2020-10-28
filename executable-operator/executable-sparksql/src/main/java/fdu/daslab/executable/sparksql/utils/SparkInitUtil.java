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
    private static SparkSession sparkSession;

    public static SparkSession getDefaultSparkSession() {
        if (sparkSession == null) {
            sparkSession = SparkSession.builder().master("local").appName("SparkSQLStage").getOrCreate();
        }
        return sparkSession;
    }

    public static SparkSession setSparkSession(String master, String appName) {
        sparkSession = SparkSession.builder().master(master).appName(appName).getOrCreate();
        return sparkSession;
    }
}
