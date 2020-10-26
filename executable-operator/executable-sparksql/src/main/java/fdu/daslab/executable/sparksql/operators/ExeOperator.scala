package fdu.daslab.executable.sparksql.operators

import java.util

import fdu.daslab.executable.basic.model.{OperatorBase, ParamsModel, ResultModel}
import fdu.daslab.executable.sparksql.utils.SparkInitUtil
import org.apache.spark.sql.DataFrame

/**
 * SparkSQL平台的sql语句执行算子
 *
 * @author 刘丰艺
 * @since 2020/10/8 9:30 PM
 * @version 1.0
 */
class ExeOperator(name: String, id: String,
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
    val result = sparkSession.sql(this.params.get("sqlText"))
    this.setOutputData("result", result)
  }
}
