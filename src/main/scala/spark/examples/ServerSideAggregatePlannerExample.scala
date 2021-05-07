package spark.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SparkSession, SparkSessionExtensions}
import spark.examples.EngineRelation.getAggregateEntities

object ServerSideAggregatePlannerExample extends App {
  val sparkSesson = SparkSession.builder()
    .master("local")
    .appName(getClass.getName.split('.').last)
    // Add our custom plan resolver for the subset of queries to our engine
    .withExtensions((extensions: SparkSessionExtensions) => {
      extensions.injectResolutionRule(EnginePlanner)
    })
    .getOrCreate()

  import sparkSesson.implicits._

  // Raw data that is internally the source of truth for the optimized aggregation engine
  Seq(
    ("A", 100),
    ("A", 80),
    ("B", 50),
    ("B", 40),
    ("C", 25),
    ("C", 12)
  ).toDF("item", "units")
    .createOrReplaceTempView("data_raw")

  // Catalog entry of optimized aggregation engine table
  sparkSesson.createDataFrame(sparkSesson.sparkContext.emptyRDD[Row], StructType(Seq(
    StructField("item", StringType),
    StructField("units", IntegerType)
  )))
    .createOrReplaceTempView("data_engine")

  val sqlTemplate =
    s"""SELECT item, SUM(units) AS units
      |FROM {{table}}
      |WHERE item IN ('A', 'B')
      |GROUP BY item
      |ORDER BY units DESC
      |LIMIT 1
      |""".stripMargin

  println("================================================================================")
  println("== Raw Data Plan");
  println("================================================================================")
  val fromRaw = sparkSesson.sql(sqlTemplate.replace("{{table}}", "data_raw"))
  fromRaw.explain(true)
  fromRaw.show()

  println("================================================================================")
  println("== Engine Evaluated Aggregate Plan");
  println("================================================================================")
  val fromEngine = sparkSesson.sql(sqlTemplate.replace("{{table}}", "data_engine"))
  fromEngine.explain(true)
  fromEngine.show()
}

/**
 * Plan optimizer that looks for top level or nested queries
 * that match the pattern that our aggregate engine supports:
 *
 * 1. We have to know the table/relation being selected from...
 *    a. Is in the SparkSession catalog
 *    b. Is stored backed by our engine
 * 2. If we find a relation match we need to grab the sub-tree including...
 *    a. The aggregate "GROUP BY" and expressions
 *    b. The filter
 *    c. The sort
 *    d. The limit
 * 3. All of the above need to be handed to the engine in a new
 *    relation to evaluate internally with the optimized engine
 * 4. The new relation will hand back an aggregates, filtered, sorted, etc dataset
 *
 * @param sparkSession
 */
case class EnginePlanner(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    val newPlan = plan transformDown {
      /**
       * We actually support more than this exact pattern by
       * understanding default for sorting, limits, etc.  But
       * this illustrates the plan sub-tree replacement we need
       * to do.
       *
       * Aggregate with...
       * sort = provided
       * limit = provided by GlobalLimit with nested LocalLimit
       */
      case globalLimit @GlobalLimit(
        limitExpr,
        localLimit @LocalLimit(
          _,
          sort @Sort(
            sortOrder,
            _,
            aggregate @Aggregate(
              groupingExpressions,
              aggregateExpressions,
              filter @Filter(
                condition,
                SubqueryAlias(
                  alias @ AliasIdentifier(tableName, _),
                  _
                )
              )
            )
          )
        )
      ) if sparkSession.catalog.tableExists(tableName) =>
        buildRelation(
          tableName,
          condition,
          groupingExpressions,
          aggregateExpressions,
          Some(sortOrder),
          Some(limitExpr)
        ).map(relation => {
          globalLimit.copy(
            child = localLimit.copy(
              child = sort.copy(
                child = relation
              )
            )
          )
        }).getOrElse(globalLimit)
      /**
       * Not a plan we can change
       */
      case other =>
        other
    }
    if (newPlan fastEquals plan) {
      plan
    } else {
      newPlan
    }
  }

  private[this] def buildRelation(
    tableName: String,
    filter: Expression,
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    sortOrder: Option[Seq[SortOrder]] = None,
    limitExpr: Option[Expression] = None
  ): Option[LogicalPlan] = {
    EngineRelation.checkAndBuild(
      sparkSession,
      tableName,
      filter,
      groupingExpressions,
      aggregateExpressions,
      sortOrder,
      limitExpr
    ).map { aresRelation =>
      SubqueryAlias(tableName, LogicalRelation(
        relation = aresRelation,
        output = aresRelation.output,
        catalogTable = None,
        isStreaming = false
      ))
    }
  }
}

/**
 * This relation simulates what would ultimately be a call to
 * our engine that is optimized for aggregate queries handling
 * all of the aggregate, sort, limit, filtering, etc logic internally
 * and just returning the aggregate result.
 *
 * @param tableName
 * @param filter
 * @param groupingExpressions
 * @param aggregateExpressions
 * @param sortOrder
 * @param limitExpr
 * @param sparkSession
 */
case class EngineRelation(
  tableName: String,
  filter: Expression,
  groupingExpressions: Seq[Expression],
  aggregateExpressions: Seq[NamedExpression],
  sortOrder: Option[Seq[SortOrder]] = None,
  limitExpr: Option[Expression] = None
)(@transient val sparkSession: SparkSession)
  extends BaseRelation
    with TableScan {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  // The real implementation would talk to the engine to get the real schema
  override def schema: StructType = StructType(Seq(
    StructField("item", StringType),
    StructField("units", IntegerType)
  ))

  /**
   * We're merging a both an aggregate and a relation so we need to combine
   * the outputs to keep the plan resolved and in sync.
   *
   * 1. Reuse the original relation output to preserve the exact fields
   *    expected by the downstream plan that's already resolved.
   * 2. Reuse the expression IDs from the original aggregate expressions
   *    for the same reason.
   */
  private[this] val entitiesMaybe = EngineRelation.getAggregateEntities(groupingExpressions, aggregateExpressions)
  val output: Seq[AttributeReference] = schema.flatMap { field =>
    for {
      entities <- entitiesMaybe.toSeq
      entity <- entities.find(field.name == _._1)
    } yield AttributeReference(
      name = field.name,
      dataType = field.dataType,
      nullable = field.nullable,
      metadata = field.metadata
    )(exprId = entity._3)
  }

  /** The real implementation would form and make a call to the engine including
   * all of the passed in details to push down to the engine to handle and
   * return the final aggregate result.  A likely implementation would be taking
   * these pieces and rebuilding the ful SQL SELECT including pieces like the
   * GROUP BY to send to the engine.
   */
  override def buildScan(): RDD[Row] = {
    sqlContext.sparkContext.parallelize(Seq(
      Row("A", 180)
    ))
  }
}

// Utility to create the engine relation
object EngineRelation {
  val ENGINE_TABLES = Set("data_engine")

  def checkAndBuild(
    sparkSession: SparkSession,
    tableName: String,
    filter: Expression,
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    sortOrder: Option[Seq[SortOrder]] = None,
    limitExpr: Option[Expression] = None
  ): Option[EngineRelation] = {
    if (ENGINE_TABLES.contains(tableName)) {
      Some(EngineRelation(
        tableName,
        filter,
        groupingExpressions,
        aggregateExpressions,
        sortOrder,
        limitExpr
      )(sparkSession))
    } else {
      None
    }
  }

  def getAggregateEntities(
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression]
  ): Option[Seq[(String, DataType, ExprId)]] = {

    if (groupingExpressions.nonEmpty && aggregateExpressions.nonEmpty) {
      // Dimensions / what we're grouping by must be named and resolved
      val dimensions = groupingExpressions.flatMap {
        case named: NamedExpression if named.resolved =>
          Some((named.name, named.dataType, named.exprId))
        case other =>
          None
      }

      // Metrics must be resolved and be supported by buildMetric()
      val metrics = aggregateExpressions.zipWithIndex.flatMap {
        // Drop the expressions for the dimensions
        case (dimension, _) if dimensions.exists(_._1 == dimension.name) =>
          None
        case (metric, _) if metric.resolved =>
          Some((
            metric.name,
            metric.dataType,
            metric.exprId
          ))
        case (other, i) =>
          Some(other.name, DoubleType, null)
      }
      Some(dimensions ++ metrics)
    } else {
      None
    }
  }
}
