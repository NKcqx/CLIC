package fdu.daslab.executable.sparksql.operators
import java.util

import fdu.daslab.executable.basic.model.{OperatorBase, ParamsModel, ResultModel}
import fdu.daslab.executable.sparksql.utils.{SQLAdapter, SparkInitUtil}
import org.apache.spark.sql.DataFrame

/**
 * SparkSQL平台的读取数据源算子
 *
 * @author 刘丰艺
 * @since 2020/10/8 9:30 PM
 * @version 1.0
 */
class FileSource(name: String, id: String,
                 inputKeys: util.List[String], outputKeys: util.List[String],
                 params: util.Map[String, String])
  extends OperatorBase[DataFrame, DataFrame](name: String, id: String,
  inputKeys: util.List[String], outputKeys: util.List[String],
  params: util.Map[String, String]) {

  /**
   * 算子的执行
   *
   * @param inputArgs 参数列表
   * @param result    返回的结果
   */
  override def execute(inputArgs: ParamsModel, result: ResultModel[DataFrame]): Unit = {
    val sparkSession = SparkInitUtil.getDefaultSparkSession()
    var df: DataFrame = null;

    val inputPath = this.params.get("inputPath")

    val tableName = inputPath.substring(inputPath.lastIndexOf("/")+1, inputPath.lastIndexOf("."))
    val fileType = inputPath.substring(inputPath.lastIndexOf("."), inputPath.length)

    fileType match {
      case ".csv" => {
        df = sparkSession.read.format("csv").option("header","true").load(inputPath)
      }
      case ".txt" => {
        df = sparkSession.read.option("header", "true").csv(inputPath)
      }
      case ".json" => {
        df = sparkSession.read.json(inputPath).toDF()
      }
    }
    df.createTempView(tableName)
    this.setOutputData("result", df)
  }
}
