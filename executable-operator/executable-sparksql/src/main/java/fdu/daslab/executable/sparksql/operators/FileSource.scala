package fdu.daslab.executable.sparksql.operators
import java.util
import java.util.{List, Map}

import fdu.daslab.executable.basic.model.{OperatorBase, ParamsModel, ResultModel}
import fdu.daslab.executable.sparksql.utils.SparkInitUtil
import org.apache.spark.rdd.RDD

/**
 * SparkSQL平台的读取数据源算子
 * （现在sql执行功能也在这个算子中）
 *
 * @author 刘丰艺
 * @since 2020/10/8 9:30 PM
 * @version 1.0
 */
class FileSource(name: String, id: String,
                 inputKeys: util.List[String], outputKeys: util.List[String],
                 params: util.Map[String, String])
  extends OperatorBase[RDD[Array[String]], RDD[Array[String]]](name: String, id: String,
  inputKeys: util.List[String], outputKeys: util.List[String],
  params: util.Map[String, String]) {

  /**
   * 算子的执行
   *
   * @param inputArgs 参数列表
   * @param result    返回的结果
   */
  override def execute(inputArgs: ParamsModel, result: ResultModel[RDD[Array[String]]]): Unit = {
    val sparkSession = SparkInitUtil.getDefaultSparkSession()

    //val rdd = sparkSession.sparkContext.textFile(this.params.get("inputPath")).map(_.split(","))

    val tableNum = this.params.get("tableNum").toInt
    // 循环遍历读取用户输入的多个数据源文件
    for(i <- 1 to tableNum) {
      val inputPath = this.params.get("inputPath" + i)
      val tableName = this.params.get("tableName" + i)
      // 通过截取文件名后缀获取该文件类型
      val fileType = inputPath.substring(inputPath.indexOf("."), inputPath.length)
      fileType match {
        case ".csv" => {
          sparkSession.read.format("csv").option("header","true").load(inputPath).createTempView(tableName)
        }
        case ".txt" => {

        }
        case ".json" => {

        }
        case ".parquet" => {

        }
      }
    }

    val sqlText = this.params.get("sqlText")
    sparkSession.sql(sqlText).show()

    //this.setOutputData("result", rdd)
  }
}
