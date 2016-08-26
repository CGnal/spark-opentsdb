/*
 * Copyright 2016 CGnal S.p.A.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cgnal.spark.opentsdb

import java.sql.Timestamp

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext, SaveMode }

class DefaultSource extends RelationProvider with CreatableRelationProvider {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {

    val METRIC = "opentsdb.metric"
    val TAGS = "opentsdb.tags"
    val INTERVAL = "opentsdb.interval"
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

    val keytab = parameters.get(KEYTAB)

    val principal = parameters.get(PRINCIPAL)

    val openTSDBContext = new OpenTSDBContext(sqlContext, DefaultSource.configuration)

    keytab.foreach(openTSDBContext.keytab = _)

    principal.foreach(openTSDBContext.principal = _)

    new OpenTSDBRelation(sqlContext, openTSDBContext, metric, tags, interval)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val openTSDBRelation = createRelation(sqlContext, parameters)
    mode match {
      case SaveMode.Append => openTSDBRelation.asInstanceOf[OpenTSDBRelation].insert(data, overwrite = false)
      case _ => throw new UnsupportedOperationException("Currently, only Append is supported")
    }

    openTSDBRelation
  }
}

class OpenTSDBRelation(val sqlContext: SQLContext, openTSDBContext: OpenTSDBContext, metric: Option[String], tags: Map[String, String], interval: Option[(Long, Long)]) extends BaseRelation with TableScan with InsertableRelation {

  override def schema = StructType(
    Array(
      StructField("timestamp", TimestampType, nullable = false),
      StructField("metric", StringType, nullable = false),
      StructField("value", DoubleType, nullable = false),
      StructField("tags", DataTypes.createMapType(StringType, StringType), nullable = false)
    )
  )

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    if (overwrite) {
      throw new UnsupportedOperationException("overwrite is not yet supported")
    }
    openTSDBContext.write(data)
  }

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

@SuppressWarnings(Array("org.wartremover.warts.Var"))
object DefaultSource {
  var configuration: Option[Configuration] = None
}
