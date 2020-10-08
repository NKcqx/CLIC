package fdu.daslab.executable.sparksql.operators
import java.util
import java.util.{List, Map}

import fdu.daslab.executable.basic.model.{OperatorBase, ParamsModel, ResultModel}
import fdu.daslab.executable.sparksql.utils.SparkInitUtil
import org.apache.spark.rdd.RDD

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
    println("start read")
    val sparkSession = SparkInitUtil.getDefaultSparkSession()
    val rdd = sparkSession.sparkContext.textFile(this.params.get("inputPath")).map(_.split(","))
    this.setOutputData("result", rdd)
    println("read success")
  }
}
