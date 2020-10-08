package fdu.daslab.executable.sparksql.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
 * 初始化spark需要的一些方法
 *
 * @author 刘丰艺
 * @since 2020/10/8 5:30 PM
 * @version 1.0
 */
object SparkInitUtil {

  private val sparkContext = SparkSession.builder()
    .master("local").appName("SparkSQLStage").getOrCreate()
    .sparkContext

  private val sparkSession = SparkSession.builder()
    .master("local").appName("SparkSQLStage").getOrCreate()

  def getDefaultSparkContext(): SparkContext = {
    sparkContext
  }

  def getDefaultSparkSession(): SparkSession = {
    sparkSession
  }
}
