package fdu.daslab.executable.sparksql.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
 * Logical Plan与J老师组对接
 * 本部分的Logical Plan指的是Spark SQL的Logical Plan
 * 现在没有用，暂时搁置
 *
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/10/12 9:30 下午
 */
object SQLAdapter {

  /**
   * 1. 将用户写的sql语句转成Logical Plan
   * 2. 优化Logical Plan
   * 3. 将优化后的Logical Plan转回sql语句
   *
   * @param 使用Spark SQL相关操作需要一个SparkSession对象
   * @param 用户写的sql语句
   * @return 优化后的Logical Plan对应的sql语句
   */
  def getOptimizedSqlText(sparkSession: SparkSession, sqlText: String, tableNames: Array[String]): String = {
    // Spark SQL
    val plan = sparkSession.sessionState.optimizer.execute(
      sparkSession.sharedState.cacheManager.useCachedData(
        sparkSession.sessionState.analyzer.executeAndCheck(
          sparkSession.sessionState.sqlParser.parsePlan(sqlText))))
//    visitLogicalPlan(plan)
//    val newSqlText = new SQLBuilder(plan, tableNames).toSQL(plan)
//    newSqlText
    sqlText
  }

  /**
   * 遍历荆老师组返回的plan tree
   * @param plan
   */
  def visitLogicalPlan(plan: LogicalPlan): Unit = {
    val children = plan.children
    if (children == null || children.length == 0) {
      return
    }
    for (child <- children) {
      child match {
        case Filter(condition, child) => condition.sql
        case Project(projectList, child) => projectList
        case Join(left, right, joinType, condition) => {
          left
          right
          joinType
          condition
        }
        case LogicalRelation(relation, output, catalogTable, isStreaming) => {
          relation.schema.toDDL
          output
          catalogTable
          isStreaming
        }
        case _ => child.getClass
      }
      // 递归遍历子节点
      visitLogicalPlan(child)
    }
  }
}
