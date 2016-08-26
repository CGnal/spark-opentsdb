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

import java.io.File
import java.nio.file.{ Files, Paths }
import java.sql.Timestamp
import java.time.{ Instant, LocalDateTime, ZoneId, ZonedDateTime }
import java.util

import com.cloudera.sparkts.{ DateTimeIndex, Frequency, TimeSeriesRDD }
import net.opentsdb.core.{ IllegalDataException, Internal, TSDB }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.streaming.dstream.DStream
import shaded.org.hbase.async.KeyValue

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.language.{ postfixOps, reflectiveCalls }

final case class DataPoint[T <: AnyVal](metric: String, timestamp: Long, value: T, tags: Map[String, String]) extends Serializable

@SuppressWarnings(Array("org.wartremover.warts.Var"))
object OpenTSDBContext {

  var tsdbTable = "tsdb"

  var tsdbUidTable = "tsdb-uid"

  var saltWidth: Int = 0

  var saltBuckets: Int = 0

}

class OpenTSDBContext(@transient sqlContext: SQLContext, @transient configuration: Option[Configuration] = None) extends Serializable {

  @transient lazy val log = Logger.getLogger(getClass.getName)

  val hbaseContext = new HBaseContext(sqlContext.sparkContext, configuration.getOrElse(new Configuration()))

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  var tsdbTable = OpenTSDBContext.tsdbTable

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  var tsdbUidTable = OpenTSDBContext.tsdbUidTable

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  var saltWidth: Int = OpenTSDBContext.saltWidth

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  var saltBuckets: Int = OpenTSDBContext.saltBuckets

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  protected var keytab_ : Option[Broadcast[Array[Byte]]] = None

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  protected var principal_ : Option[String] = None

  def keytab = keytab_.getOrElse(throw new Exception("keytab has not been defined"))

  def keytab_=(keytab: String) = {
    val keytabPath = new File(keytab).getAbsolutePath
    val byteArray = Files.readAllBytes(Paths.get(keytabPath))
    keytab_ = Some(sqlContext.sparkContext.broadcast(byteArray))
  }

  def principal = principal_.getOrElse(throw new Exception("principal has not been defined"))

  def principal_=(principal: String) = principal_ = Some(principal)

  def loadTimeSeriesRDD(
    interval: Option[(Long, Long)],
    frequency: Frequency,
    metrics: List[(String, Map[String, String])]
  ): TimeSeriesRDD[String] = {

    val startDateEpochInMillis: Long = new Timestamp(interval.getOrElse(throw new Exception)._1.toLong * 1000).getTime
    val endDateEpochInInMillis: Long = new Timestamp(interval.getOrElse(throw new Exception)._2.toLong * 1000).getTime - 1

    LocalDateTime.ofInstant(Instant.ofEpochMilli(startDateEpochInMillis), ZoneId.of("Z"))

    val startZoneDate = ZonedDateTime.of(LocalDateTime.ofInstant(Instant.ofEpochMilli(startDateEpochInMillis), ZoneId.of("Z")), ZoneId.of("Z"))
    val endZoneDate = ZonedDateTime.of(LocalDateTime.ofInstant(Instant.ofEpochMilli(endDateEpochInInMillis), ZoneId.of("Z")), ZoneId.of("Z"))

    val index = DateTimeIndex.uniformFromInterval(startZoneDate, endZoneDate, frequency, ZoneId.of("UTC")).atZone(ZoneId.of("UTC"))

    val dfs: List[DataFrame] = metrics.map(m => loadDataFrame(
      m._1,
      m._2,
      interval
    ))

    val initDF = dfs.headOption.getOrElse(throw new Exception("There must be at least one dataframe"))
    val otherDFs = dfs.drop(1)

    val observations = if (otherDFs.isEmpty)
      initDF
    else
      otherDFs.fold(initDF)((df1, df2) => df1.unionAll(df2))

    TimeSeriesRDD.timeSeriesRDDFromObservations(
      index,
      observations,
      "timestamp",
      "metric",
      "value"
    )
  }

  def loadDataFrame(
    metricName: String,
    tags: Map[String, String] = Map.empty[String, String],
    interval: Option[(Long, Long)] = None
  ): DataFrame = {
    val schema = StructType(
      Array(
        StructField("timestamp", TimestampType, nullable = false),
        StructField("metric", StringType, nullable = false),
        StructField("value", DoubleType, nullable = false),
        StructField("tags", DataTypes.createMapType(StringType, StringType), nullable = false)
      )
    )

    val rowRDD = load(metricName, tags, interval, ConvertToDouble).map[Row] {
      dp =>
        Row(
          new Timestamp(dp.timestamp),
          dp.metric,
          dp.value.asInstanceOf[Double],
          dp.tags
        )
    }
    sqlContext.createDataFrame(rowRDD, schema)
  }

  def load(
    metricName: String,
    tags: Map[String, String] = Map.empty[String, String],
    interval: Option[(Long, Long)] = None,
    conversionStrategy: ConversionStrategy = NoConversion
  ): RDD[DataPoint[_ <: AnyVal]] = {

    log.info("Loading metric and tags uids")
    val uidScan = getUIDScan(metricName, tags)
    val tsdbUID = hbaseContext.hbaseRDD(TableName.valueOf(tsdbUidTable), uidScan).asInstanceOf[RDD[(ImmutableBytesWritable, Result)]]
    val metricsUID: Array[Array[Byte]] = tsdbUID.map(p => p._2.getValue("id".getBytes, "metrics".getBytes())).filter(_ != null).collect
    val (tagKUIDs, tagVUIDs) = if (tags.isEmpty)
      (Map.empty[String, Array[Byte]], Map.empty[String, Array[Byte]])
    else {
      (
        tsdbUID.map(p => (new String(p._1.copyBytes), p._2.getValue("id".getBytes, "tagk".getBytes))).filter(_._2 != null).collect.toMap,
        tsdbUID.map(p => (new String(p._1.copyBytes), p._2.getValue("id".getBytes, "tagv".getBytes))).filter(_._2 != null).collect.toMap
      )
    }
    if (metricsUID.length == 0)
      throw new Exception(s"Metric not found: $metricName")
    log.info("Loading metric and tags uids: done")

    val rows = if (saltWidth == 0) {
      log.trace("computing hbase rows without salting")
      val metricScan = getMetricScan(
        -1: Byte,
        tags,
        metricsUID.last,
        tagKUIDs,
        tagVUIDs,
        interval
      )
      hbaseContext.hbaseRDD(TableName.valueOf(tsdbTable), metricScan).asInstanceOf[RDD[(ImmutableBytesWritable, Result)]]
    } else {
      assert(saltWidth == 1)
      assert(saltBuckets >= 1)
      log.trace("computing hbase rows with salting")
      val rdds = (0 until saltBuckets) map {
        bucket =>
          val metricScan = getMetricScan(
            bucket.toByte,
            tags,
            metricsUID.last,
            tagKUIDs,
            tagVUIDs,
            interval
          )
          hbaseContext.hbaseRDD(TableName.valueOf(tsdbTable), metricScan).asInstanceOf[RDD[(ImmutableBytesWritable, Result)]]
      } toList

      val initRDD = rdds.headOption.getOrElse(throw new Exception("There must be at least one RDD"))
      val otherRDDs = rdds.drop(1)

      if (otherRDDs.isEmpty)
        initRDD
      else
        otherRDDs.fold(initRDD)((rdd1, rdd2) => rdd1.union(rdd2))
    }

    val rdd = rows.mapPartitions[Iterator[DataPoint[_ <: AnyVal]]](f = iterator => {
      TSDBClientManager(
        keytab = keytab_,
        principal = principal_,
        hbaseContext = hbaseContext,
        tsdbTable = tsdbTable,
        tsdbUidTable = tsdbUidTable,
        saltWidth = saltWidth,
        saltBuckets = saltBuckets
      )
      new Iterator[Iterator[DataPoint[_ <: AnyVal]]] {
        val i = iterator.map(row => process(row, TSDBClientManager.tsdb.getOrElse(throw new Exception("the TSDB client instance has not been initialised correctly")), interval, conversionStrategy))

        override def hasNext =
          if (!i.hasNext) {
            log.trace("iterating done, about to shutdown the TSDB client instance")
            TSDBClientManager.shutdown()
            false
          } else
            i.hasNext

        override def next() = i.next()
      }
    }, preservesPartitioning = true)

    rdd.flatMap(identity[Iterator[DataPoint[_ <: AnyVal]]])
  }

  private def process(row: (ImmutableBytesWritable, Result), tsdb: TSDB, interval: Option[(Long, Long)], conversionStrategy: ConversionStrategy): Iterator[DataPoint[_ <: AnyVal]] = {
    log.trace("processing row")
    val key = row._1.get()
    val metric = Internal.metricName(tsdb, key)
    val baseTime = Internal.baseTime(tsdb, key)
    val tags = Internal.getTags(tsdb, key).asScala
    val dps = new ListBuffer[DataPoint[_ <: AnyVal]]
    row._2.rawCells().foreach[Unit](cell => {
      val family = util.Arrays.copyOfRange(cell.getFamilyArray, cell.getFamilyOffset, cell.getFamilyOffset + cell.getFamilyLength)
      val qualifier = util.Arrays.copyOfRange(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierOffset + cell.getQualifierLength)
      val value = util.Arrays.copyOfRange(cell.getValueArray, cell.getValueOffset, cell.getValueOffset + cell.getValueLength)
      val kv = new KeyValue(key, family, qualifier, cell.getTimestamp, value)
      if (qualifier.length == 2 || qualifier.length == 4 && Internal.inMilliseconds(qualifier)) {
        val cell = Internal.parseSingleValue(kv)
        if (cell == null) {
          throw new IllegalDataException("Unable to parse row: " + kv)
        }
        val ts = cell.absoluteTimestamp(baseTime)
        val isInTheInterval = interval.fold(true)(
          interval => if (Internal.inMilliseconds(cell.qualifier()))
            ts >= interval._1 * 1000 && ts < interval._2 * 1000
          else
            ts >= interval._1 && ts < interval._2
        )
        if (isInTheInterval)
          dps += (conversionStrategy match {
            case ConvertToDouble => DataPoint(metric, ts, cell.parseValue().doubleValue(), tags.toMap)
            case NoConversion => if (cell.isInteger)
              DataPoint(metric, cell.absoluteTimestamp(baseTime), cell.parseValue().longValue(), tags.toMap)
            else
              DataPoint(metric, cell.absoluteTimestamp(baseTime), cell.parseValue().doubleValue(), tags.toMap)
          })
      } else {
        // compacted column
        log.trace("processing compacted row")
        val cells = new ListBuffer[Internal.Cell]
        try {
          cells ++= Internal.extractDataPoints(kv).asScala
        } catch {
          case e: IllegalDataException =>
            throw new IllegalDataException(Bytes.toStringBinary(key), e)
        }
        cells.foreach[Unit](cell => {
          val ts = cell.absoluteTimestamp(baseTime)
          val isInTheInterval = interval.fold(true)(
            interval => if (Internal.inMilliseconds(cell.qualifier()))
              ts >= interval._1 * 1000 && ts < interval._2 * 1000
            else
              ts >= interval._1 && ts < interval._2
          )
          if (isInTheInterval)
            dps += (conversionStrategy match {
              case ConvertToDouble => DataPoint(metric, ts, cell.parseValue().doubleValue(), tags.toMap)
              case NoConversion => if (cell.isInteger)
                DataPoint(metric, ts, cell.parseValue().longValue(), tags.toMap)
              else
                DataPoint(metric, ts, cell.parseValue().doubleValue(), tags.toMap)
            })
          ()
        })
        log.trace(s"processed ${cells.length} cells")
      }
      ()
    })
    log.trace("processing row: done")
    dps.iterator
  }

  def write[T <: AnyVal](timeseries: RDD[DataPoint[T]])(implicit writeFunc: (Iterator[DataPoint[T]], TSDB) => Unit): Unit = {
    timeseries.foreachPartition(it => {
      TSDBClientManager(
        keytab = keytab_,
        principal = principal_,
        hbaseContext = hbaseContext,
        tsdbTable = tsdbTable,
        tsdbUidTable = tsdbUidTable,
        saltWidth = saltWidth,
        saltBuckets = saltBuckets
      )
      writeFunc(
        new Iterator[DataPoint[T]] {
          override def hasNext =
            if (!it.hasNext) {
              log.trace("iterating done, about to shutdown the TSDB client instance")
              TSDBClientManager.shutdown()
              false
            } else
              it.hasNext

          override def next() = it.next()
        }, TSDBClientManager.tsdb.getOrElse(throw new Exception("the TSDB client instance has not been initialised correctly"))
      )
    })
  }

  def write(timeseries: DataFrame)(implicit writeFunc: (Iterator[DataPoint[Double]], TSDB) => Unit): Unit = {
    assert(timeseries.schema == StructType(
      Array(
        StructField("timestamp", TimestampType, nullable = false),
        StructField("metric", StringType, nullable = false),
        StructField("value", DoubleType, nullable = false),
        StructField("tags", DataTypes.createMapType(StringType, StringType), nullable = false)
      )
    ))
    timeseries.foreachPartition(it => {
      TSDBClientManager(
        keytab = keytab_,
        principal = principal_,
        hbaseContext = hbaseContext,
        tsdbTable = tsdbTable,
        tsdbUidTable = tsdbUidTable,
        saltWidth = saltWidth,
        saltBuckets = saltBuckets
      )
      writeFunc(
        new Iterator[DataPoint[Double]] {
          override def hasNext =
            if (!it.hasNext) {
              log.trace("iterating done, about to shutdown the TSDB client instance")
              TSDBClientManager.shutdown()
              false
            } else
              it.hasNext

          override def next() = {
            val row = it.next()
            DataPoint(
              row.getAs[String]("metric"),
              row.getAs[Timestamp]("timestamp").getTime,
              row.getAs[Double]("value"),
              row.getAs[Map[String, String]]("tags")
            )
          }
        }, TSDBClientManager.tsdb.getOrElse(throw new Exception("the TSDB client instance has not been initialised correctly"))
      )
    })

  }

  def streamWrite[T <: AnyVal](dstream: DStream[DataPoint[T]])(implicit writeFunc: (Iterator[DataPoint[T]], TSDB) => Unit): Unit = {
    dstream foreachRDD {
      timeseries =>
        timeseries foreachPartition {
          it =>
            TSDBClientManager(
              keytab = keytab_,
              principal = principal_,
              hbaseContext = hbaseContext,
              tsdbTable = tsdbTable,
              tsdbUidTable = tsdbUidTable,
              saltWidth = saltWidth,
              saltBuckets = saltBuckets
            )
            writeFunc(
              new Iterator[DataPoint[T]] {
                override def hasNext =
                  if (!it.hasNext) {
                    log.trace("iterating done, about to shutdown the TSDB client instance")
                    TSDBClientManager.shutdown()
                    false
                  } else
                    it.hasNext

                override def next() = it.next()
              }, TSDBClientManager.tsdb.getOrElse(throw new Exception("the TSDB client instance has not been initialised correctly"))
            )
        }
    }
  }

}
