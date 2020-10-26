package fdu.daslab.executable.sparksql.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
 * CLIC组与Semantics组对接
 * 现在没有用，暂时搁置
 *
 * @author 刘丰艺
 * @version 1.0
 * @since 2020/10/12 9:30 下午
 */
object SQLAdapter {

  /**
   * CLIC给Semantics传入一条sql语句
   * Semantics给CLIC返回一棵优化后的Tree
   */
  def getOptimizedSqlText(sparkSession: SparkSession, sqlText: String): LogicalPlan = {
    // Semantics要做的操作内容跟下面Spark SQL的操作一样
    val plan = sparkSession.sessionState.optimizer.execute(
      sparkSession.sharedState.cacheManager.useCachedData(
        sparkSession.sessionState.analyzer.executeAndCheck(
          sparkSession.sessionState.sqlParser.parsePlan(sqlText))))
    plan
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
        case Filter(condition, child) =>
        case Project(projectList, child) =>
        case Join(left, right, joinType, condition) =>
        case LogicalRelation(relation, output, catalogTable, isStreaming) =>
        case _ =>
      }
      // 递归遍历子节点
      visitLogicalPlan(child)
    }
  }
}
