/*
 * Copyright 2016 CGnal S.p.A.
 *
 */

package com.cgnal.spark.opentsdb

import java.sql.Timestamp

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

/**
 * Data source for integration with Spark's [[DataFrame]] API.
 *
 * Serves as a factory for [[OpenTSDBRelation]] instances for Spark. Spark will
 * automatically look for a [[RelationProvider]] implementation named
 * `DefaultSource` when the user specifies the path of a source during DDL
 * operations through [[org.apache.spark.sql.DataFrameReader.format]].
 */
class DefaultSource extends RelationProvider with CreatableRelationProvider {

  /**
   * Construct a BaseRelation using the provided context and parameters.
   *
   * @param sqlContext SparkSQL context
   * @param parameters parameters given to us from SparkSQL
   * @return a BaseRelation Object
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {

    val METRIC = "opentsdb.metric"
    val TAGS = "opentsdb.tags"
    val INTERVAL = "opentsdb.interval"
    val KEYTABLOCALTEMPDIR = "opentsdb.keytabLocalTempDir"
    val KEYTAB = "opentsdb.keytab"
    val PRINCIPAL = "opentsdb.principal"

    val metric = parameters.get(METRIC)

    val tags = parameters.get(TAGS).fold(Map.empty[String, String])(
      _.split(",").map(tk => {
      val p = tk.trim.split("->")
      (p(0).trim, p(1).trim)
    }).toMap
    )

    val interval = parameters.get(INTERVAL).fold(None: Option[(Long, Long)])(
      interval => {
        val a = interval.trim.split(":").map(_.trim.toLong)
        Some((a(0), a(1)))
      }
    )

    val keytabLocalTempDir = parameters.get(KEYTABLOCALTEMPDIR)

    val keytab = parameters.get(KEYTAB)

    val principal = parameters.get(PRINCIPAL)

    val openTSDBContext = new OpenTSDBContext(SparkSession.builder().getOrCreate())

    keytabLocalTempDir.foreach(openTSDBContext.keytabLocalTempDir = _)

    keytab.foreach(openTSDBContext.keytab = _)

    principal.foreach(openTSDBContext.principal = _)

    new OpenTSDBRelation(sqlContext, openTSDBContext, metric, tags, interval)
  }

  /**
   * Creates a relation and inserts data to specified table.
   *
   * @param sqlContext The Spark SQL Context
   * @param mode       Only Append mode is supported. It will upsert or insert data
   *                   to an existing table, depending on the upsert parameter.
   * @param parameters Necessary parameters for OpenTSDB
   * @param data       Dataframe to save into OpenTSDB
   * @return returns populated base relation
   */
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val openTSDBRelation = createRelation(sqlContext, parameters)
    mode match {
      case SaveMode.Append => openTSDBRelation.asInstanceOf[OpenTSDBRelation].insert(data, overwrite = false)
      case _ => throw new UnsupportedOperationException("Currently, only Append is supported")
    }

    openTSDBRelation
  }
}

/**
 * Implementation of Spark BaseRelation.
 *
 * @param metric          metric name
 * @param tags            timeseries tags
 * @param interval        timeseries interval
 * @param openTSDBContext OpenTSDBContext context
 * @param sqlContext      SparkSQL context
 */
class OpenTSDBRelation(val sqlContext: SQLContext, openTSDBContext: OpenTSDBContext, metric: Option[String], tags: Map[String, String], interval: Option[(Long, Long)]) extends BaseRelation with TableScan with InsertableRelation {

  /**
   * Generates a SparkSQL schema object so SparkSQL knows what is being
   * provided by this BaseRelation.
   *
   * @return schema generated
   */
  override def schema = StructType(
    Array(
      StructField("timestamp", TimestampType, nullable = false),
      StructField("metric", StringType, nullable = false),
      StructField("value", DoubleType, nullable = false),
      StructField("tags", DataTypes.createMapType(StringType, StringType), nullable = false)
    )
  )

  /**
   * Writes data into OpenTSDB.
   * *
   * @param data [[DataFrame]] to be inserted into OpenTSDB
   * @param overwrite must be false; otherwise, throws [[UnsupportedOperationException]]
   */
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    if (overwrite) {
      throw new UnsupportedOperationException("overwrite is not yet supported")
    }
    openTSDBContext.write(data)
  }

  /**
   * Build the RDD to scan rows.
   *
   * @return RDD will all the results from OpenTSDB
   */
  override def buildScan(): RDD[Row] =
    openTSDBContext.load(
      metric.getOrElse(throw new IllegalArgumentException(s"metric name must be specified")).trim,
      tags,
      interval,
      ConvertToDouble
    ).map[Row] {
      dp =>
        Row(
          new Timestamp(dp.timestamp),
          dp.metric,
          dp.value.asInstanceOf[Double],
          dp.tags
        )
    }

}

trait OpenTSDBConfigurator { this: Serializable =>

  def configuration: Configuration

}

@SuppressWarnings(Array("org.wartremover.warts.Var"))
object DefaultSourceConfigurator extends OpenTSDBConfigurator with Serializable {

  @transient private var _configuration: Option[Configuration] = None

  private def updateConf(conf: Configuration = HBaseConfiguration.create()) = {
    _configuration = Some { conf }
    conf
  }

  def configuration: Configuration = _configuration match {
    case Some(conf) => conf
    case None | null => updateConf()
  }

}
