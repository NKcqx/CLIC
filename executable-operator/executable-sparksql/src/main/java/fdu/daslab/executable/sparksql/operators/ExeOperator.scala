package fdu.daslab.executable.sparksql.operators

import java.util

import fdu.daslab.executable.basic.model.{OperatorBase, ParamsModel, ResultModel}
import fdu.daslab.executable.sparksql.utils.SparkInitUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class ExeOperator(name: String, id: String,
                  inputKeys: util.List[String], outputKeys: util.List[String],
                  params: util.Map[String, String])
  extends OperatorBase[RDD[Array[String]], RDD[Array[String]]](name: String, id: String,
  inputKeys: util.List[String], outputKeys: util.List[String],
  params: util.Map[String, String]) {

  case class Person(id: Int, name: String)

  /**
   * 算子的执行
   *
   * @param inputArgs 参数列表
   * @param result    返回的结果
   */
  override def execute(inputArgs: ParamsModel, result: ResultModel[RDD[Array[String]]]): Unit = {
    println("start execute")
    val sparkSession = SparkInitUtil.getDefaultSparkSession()
//    val id = new StructField("id", StringType, true)
//    val name = new StructField("name", StringType, true)
//    val table_structure = new StructType(Array(id))
    val df = sparkSession.createDataFrame(this.getInputData("data").map(p => Person(p(0).toInt, p(1))))
    df.show()
    df.createTempView("person")
    sparkSession.sql(this.params.get("sqlText")).show()
    println("exe success")
  }
}
