package fdu.daslab.executable.sparksql.utils

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.Map
import scala.util.control.NonFatal
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.{CollapseProject, CombineUnions}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{ByteType, DataType, IntegerType, NullType}

/**
 * 将（Spark SQL）的Logical Plan转成SQL语句
 *
 * @author 刘丰艺
 * @since 2020/10/18 8:30 PM
 * @version 1.0
 */

case class SubqueryAlias(
                          alias: String,
                          child: LogicalPlan,
                          view: Option[TableIdentifier])
  extends UnaryNode {

  override def output: Seq[Attribute] = child.output.map(_.withQualifier(Seq(alias)))
}

/**
 * 包含一个[[LogicalPlan]]的表达式的基础接口
 */
abstract class SubqueryExpression extends PlanExpression[LogicalPlan] {
  override def withNewPlan(plan: LogicalPlan): SubqueryExpression
}

class SQLBuilder(logicalPlan: LogicalPlan, nextSubqueryId: AtomicLong, nextGenAttrId: AtomicLong,
                exprIdMap: Map[Long, Long], tableNames: Array[String]) extends Logging {

  var tableIndex = 0

  def this(logicalPlan: LogicalPlan, tableNames: Array[String]) =
    this(logicalPlan, new AtomicLong(0), new AtomicLong(0), Map.empty[Long, Long], tableNames)

//  def this(df: Dataset[_]) = this(df.queryExecution.analyzed)

  private def newSubqueryName(): String = s"gen_subquery_${nextSubqueryId.getAndIncrement()}"
  private def normalizedName(n: NamedExpression): String = synchronized {
    "gen_attr_" + exprIdMap.getOrElseUpdate(n.exprId.id, nextGenAttrId.getAndIncrement())
  }

  def toSQL(node: LogicalPlan): String = node match {
    case Distinct(p: Project) =>
      projectToSQL(p, isDistinct = true)

    case p: Project =>
      projectToSQL(p, isDistinct = false)

    case p: Aggregate =>
      aggregateToSQL(p)

    case w: Window =>
      windowToSQL(w)

    case Filter(condition, child) =>
      val whereOrHaving = child match {
        case _: Aggregate => "HAVING"
        case _ => "WHERE"
      }
      build(toSQL(child), whereOrHaving, condition.sql)

    case p @ Distinct(u: Union) if u.children.length > 1 =>
      val childrenSql = u.children.map(c => s"(${toSQL(c)})")
      childrenSql.mkString(" UNION DISTINCT ")

    case p: Union if p.children.length > 1 =>
      val childrenSql = p.children.map(c => s"(${toSQL(c)})")
      childrenSql.mkString(" UNION ALL ")

    case p: Intersect =>
      build("(" + toSQL(p.left), ") INTERSECT (", toSQL(p.right) + ")")

    case p: Except =>
      build("(" + toSQL(p.left), ") EXCEPT (", toSQL(p.right) + ")")

    case p: SubqueryAlias => build("(" + toSQL(p.child) + ")", "AS", p.alias)

    case p: Join =>
      build(
        toSQL(p.left),
        p.joinType.sql,
        "JOIN",
        toSQL(p.right),
        p.condition.map(" ON " + _.sql).getOrElse(""))

    case SQLTable(database, table, _, sample) =>
      val qualifiedName = s"${quoteIdentifier(database)}.${quoteIdentifier(table)}"
      sample.map { case (lowerBound, upperBound) =>
        val fraction = math.min(100, math.max(0, (upperBound - lowerBound) * 100))
        qualifiedName + " TABLESAMPLE(" + fraction + " PERCENT)"
      }.getOrElse(qualifiedName)

    case p: LogicalRelation => {
      val index = tableIndex
      tableIndex += 1
      if (index >= 2)
        ""
      else tableNames(index)
    }

    case Sort(orders, _, RepartitionByExpression(partitionExprs, child, _))
      if orders.map(_.child) == partitionExprs =>
      build(toSQL(child), "CLUSTER BY", partitionExprs.map(_.sql).mkString(", "))

    case p: Sort =>
      build(
        toSQL(p.child),
        if (p.global) "ORDER BY" else "SORT BY",
        p.order.map(_.sql).mkString(", ")
      )

    case p: RepartitionByExpression =>
      build(
        toSQL(p.child),
        "DISTRIBUTE BY",
        p.partitionExpressions.map(_.sql).mkString(", ")
      )

    case p: LocalRelation =>
      p.toSQL(newSubqueryName())

    case p: Range =>
      p.toSQL()

    case _ =>
      throw new UnsupportedOperationException(s"unsupported plan $node")
  }

  /**
   * 将多个String类型切片组合成一个String，每个切片之间只保留一个空格
   * 例如，`build("a", " b ", " c")` -> "a b c".
   */
  private def build(segments: String*): String =
    segments.map(_.trim).filter(_.nonEmpty).mkString(" ")

  private def projectToSQL(plan: Project, isDistinct: Boolean): String = {
    build(
      "SELECT",
      if (isDistinct) "DISTINCT" else "",
      plan.projectList.map(_.sql).mkString(", "),
      if (plan.child == OneRowRelation) "" else "FROM",
      toSQL(plan.child)
    )
  }

  private def aggregateToSQL(plan: Aggregate): String = {
    val groupingSQL = plan.groupingExpressions.map(_.sql).mkString(", ")
    build(
      "SELECT",
      plan.aggregateExpressions.map(_.sql).mkString(", "),
      if (plan.child == OneRowRelation) "" else "FROM",
      toSQL(plan.child),
      if (groupingSQL.isEmpty) "" else "GROUP BY",
      groupingSQL
    )
  }

  private def windowToSQL(w: Window): String = {
    build(
      "SELECT",
      (w.child.output ++ w.windowExpressions).map(_.sql).mkString(", "),
      if (w.child == OneRowRelation) "" else "FROM",
      toSQL(w.child)
    )
  }

  case class SQLTable(
                       database: String,
                       table: String,
                       output: Seq[Attribute],
                       sample: Option[(Double, Double)] = None) extends LeafNode {
    def withSample(lowerBound: Double, upperBound: Double): SQLTable =
      this.copy(sample = Some(lowerBound -> upperBound))
  }

}
