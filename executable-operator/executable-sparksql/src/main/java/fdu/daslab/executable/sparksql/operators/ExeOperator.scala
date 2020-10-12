package fdu.daslab.executable.sparksql.operators

import java.util

import fdu.daslab.executable.basic.model.{OperatorBase, ParamsModel, ResultModel}
import fdu.daslab.executable.sparksql.utils.SparkInitUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * SparkSQL平台的sql语句执行算子
 * （暂隐藏不用，因为现在sql执行的功能放在FileSource算子中）
 *
 * @author 刘丰艺
 * @since 2020/10/8 9:30 PM
 * @version 1.0
 */
class ExeOperator(name: String, id: String,
                  inputKeys: util.List[String], outputKeys: util.List[String],
                  params: util.Map[String, String])
  extends OperatorBase[RDD[Array[String]], RDD[Array[String]]](name: String, id: String,
  inputKeys: util.List[String], outputKeys: util.List[String],
  params: util.Map[String, String]) {

  case class TableEntity(id: Int, name: String)

  /**
   * 算子的执行
   *
   * @param inputArgs 参数列表
   * @param result    返回的结果
   */
  override def execute(inputArgs: ParamsModel, result: ResultModel[RDD[Array[String]]]): Unit = {
    println("start execute")
    val sparkSession = SparkInitUtil.getDefaultSparkSession()
//    val id = new StructField("id", IntegerType, true)
//    val name = new StructField("name", StringType, true)
//    val table_structure = new StructType(Array(id, name))
//    val df = sparkSession.createDataFrame(this.getInputData("data"), table_structure)
    val df = sparkSession.createDataFrame(this.getInputData("data").map(t => TableEntity(t(0).toInt, t(1))))
//    df.show()
    val sqlText = this.params.get("sqlText")
//    val tableName = this.params.get("tableName")
    df.createTempView("person")
    sparkSession.sql(sqlText).show()
    println("exe success")
  }
}
