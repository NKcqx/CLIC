package fdu.daslab.executable.sparksql.operators

import java.util

import fdu.daslab.executable.basic.model.{OperatorBase, ParamsModel, ResultModel}
import org.apache.spark.sql.DataFrame

/**
 * SparkSQL平台的写入文件算子
 *
 * @author 刘丰艺
 * @since 2020/10/8 9:30 PM
 * @version 1.0
 */
class FileSink(name: String, id: String,
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
    /**
     * 这种方式就是直接在HDFS上分区存储
     * 也可兼容存储在本地磁盘
     * 在这里保存为csv格式比txt好点
     */
    this.getInputData("data")
      .coalesce(this.params.get("partitionNum").toInt) // 文件分区数量
      .write
      .mode("overwrite") // 保存方式为覆盖
      .option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false") // 保存文件时去除success文件
      .option("header", "true")
      .csv(this.params.get("outputPath"))
  }
}
