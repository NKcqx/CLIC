package siamese;

import fdu.daslab.executable.spark.utils.SparkInitUtil;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.collection.JavaConversions;

import java.util.HashMap;
import java.util.Map;

/**
 * Siamese优化SQL语句之前需要知道table的表名和schema
 * 本类为他们提供所需要的表名和schema
 * <p>
 * Siamese还没有封装好他们的包给我们用，所以先用真正的Spark SQL代替
 * 逻辑阶段与物理阶段混淆，这是对接的锅，没办法
 *
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/11/29 1:53 PM
 */
public class SiameseSchema {

    // 获取表名和schema也需要先读table
    // Siamese组
    private static SparkSession sparkSession = SparkInitUtil.getDefaultSparkSession();
    // table名与table地址的映射表
    private static Map<String, String> tableAddrMap = new HashMap<>();

    public static Map<String, String> getTableAddrMap() {
        if (tableAddrMap == null) {
            tableAddrMap = new HashMap<>();
        }
        return tableAddrMap;
    }

    /**
     * 要Siamese提供优化SQL的功能，需要先读取table获取这些表的schema
     * 再将schema保存到SparkSession中（将table注册到SparkSession中）
     */
    public static void readTableToGetSchema(String tableAddr) {
        /**
         * 这里实际上进行跟后面物理平台的TableSource.java算子类似的操作
         * Siamese还没有封装好他们的包给我们用，所以先用真正的Spark SQL代替
         * 为了对接，没办法
         */
        Dataset<Row> df = null;
        String tableName = "";
        String fileType = "";

        String inputPath = tableAddr;
        if (inputPath.contains(".")) {
            tableName = inputPath.substring(inputPath.lastIndexOf("/") + 1, inputPath.lastIndexOf("."));
            fileType = inputPath.substring(inputPath.lastIndexOf("."), inputPath.length());
        } else {
            tableName = inputPath.substring(inputPath.lastIndexOf("/") + 1);
        }
        if (!tableAddrMap.containsKey(tableName)) {
            // 给之后从Siamese得到的LogicalPlan树生成的DAG使用
            tableAddrMap.put(tableName, inputPath);
            switch (fileType) {
                case ".txt":
                    df = sparkSession.read().option("header", "true").csv(inputPath);
                    break;
                case ".json":
                    df = sparkSession.read().json(inputPath).toDF();
                    break;
                default:
                    // 默认以csv方式打开数据源文件
                    // 如果源文件没有后缀，则按HDFS分布式存储来处理
                    df = sparkSession.read().format("csv").option("header", "true").load(inputPath);
            }
            try {
                // 将table注册到SparkSession中
                df.createTempView(tableName);
            } catch (AnalysisException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 针对Relation算子，获取其对应的table表名
     *
     * @param logicalPlan
     * @return
     */
    public static String getTableName(LogicalPlan logicalPlan) {
        String tableName = "";
        // Spark SQL的tempViews()保存有注册在SparkSession中的 "Relation算子-table名" 映射表
        Map<String, LogicalPlan> nameNodeMap =
                JavaConversions.mapAsJavaMap(sparkSession.sessionState().catalog().tempViews());
        for (Map.Entry<String, LogicalPlan> entry : nameNodeMap.entrySet()) {
            if (entry.getValue().equals(logicalPlan)) {
                tableName = entry.getKey();
            }
        }
        return tableName;
    }
}
