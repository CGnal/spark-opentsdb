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
import java.time._
import java.util
import java.util.TimeZone

import com.cloudera.sparkts.{ DateTimeIndex, Frequency, TimeSeriesRDD }
import net.opentsdb.core.{ IllegalDataException, Internal, TSDB }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.streaming.dstream.DStream
import shaded.org.hbase.async.KeyValue

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.language.reflectiveCalls

case class DataPoint[T <: AnyVal](metric: String, tags: Map[String, String], timestamp: Long, value: T) extends Serializable

class OpenTSDBContext(@transient sqlContext: SQLContext, @transient configuration: Configuration, dateFormat: String = "dd/MM/yyyy HH:mm") extends Serializable {

  val hbaseContext = new HBaseContext(sqlContext.sparkContext, configuration)

  var tsdbTable = "tsdb"

  var tsdbUidTable = "tsdb-uid"

  var metricWidth: Byte = 3

  var tagkWidth: Byte = 3

  var tagvWidth: Byte = 3

  private var keytab_ : Option[Broadcast[Array[Byte]]] = None

  private var principal_ : Option[String] = None

  def keytab = keytab_.get

  def keytab_=(keytab: String) = {
    val keytabPath = new File(keytab).getAbsolutePath
    val byteArray = Files.readAllBytes(Paths.get(keytabPath))
    keytab_ = Some(sqlContext.sparkContext.broadcast(byteArray))
  }

  def principal = principal_.get

  def principal_=(principal: String) = principal_ = Some(principal)

  def loadTimeSeriesRDD(
    startdate: String,
    enddate: String,
    frequency: Frequency,
    metrics: List[(String, Map[String, String])],
    dateFormat: String = this.dateFormat
  ): TimeSeriesRDD[String] = {

    val simpleDateFormat = new java.text.SimpleDateFormat(dateFormat)
    simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))

    val startDateEpochInMillis: Long = simpleDateFormat.parse(startdate).getTime
    val endDateEpochInInMillis: Long = simpleDateFormat.parse(enddate).getTime - 1

    LocalDateTime.ofInstant(Instant.ofEpochMilli(startDateEpochInMillis), ZoneId.of("Z"))

    val startZoneDate = ZonedDateTime.of(LocalDateTime.ofInstant(Instant.ofEpochMilli(startDateEpochInMillis), ZoneId.of("Z")), ZoneId.of("Z"))
    val endZoneDate = ZonedDateTime.of(LocalDateTime.ofInstant(Instant.ofEpochMilli(endDateEpochInInMillis), ZoneId.of("Z")), ZoneId.of("Z"))

    var index = DateTimeIndex.uniformFromInterval(startZoneDate, endZoneDate, frequency, ZoneId.of("UTC"))
    index = index.atZone(ZoneId.of("UTC"))

    val dfs: List[DataFrame] = metrics.map(m => loadDataFrame(
      m._1,
      m._2,
      Some(startdate),
      Some(enddate),
      dateFormat
    ))

    val initDF = dfs.headOption.get
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
    startdate: Option[String] = None,
    enddate: Option[String] = None,
    dateFormat: String = this.dateFormat
  ): DataFrame = {
    val schema = StructType(
      Array(
        StructField("timestamp", TimestampType, nullable = false),
        StructField("metric", StringType, nullable = false),
        StructField("value", DoubleType, nullable = false),
        StructField("tags", DataTypes.createMapType(StringType, StringType), nullable = false)
      )
    )

    val rowRDD = load(metricName, tags, startdate, enddate, dateFormat, ConvertToDouble).mapPartitions[Row](
      iterator => {
        TSDBClientManager(keytab = keytab_, principal = principal_, hbaseContext = hbaseContext, tsdbTable = tsdbTable, tsdbUidTable = tsdbUidTable)
        TSDBClientManager.tsdb.fold(throw _, tsdb => {
          val result = iterator.map {
            dp =>
              Row(
                new Timestamp(dp.timestamp),
                dp.metric,
                dp.value.asInstanceOf[Double],
                dp.tags
              )
          }
          result
        })
      }, preservesPartitioning = true
    )

    sqlContext.createDataFrame(rowRDD, schema)
  }

  def load(
    metricName: String,
    tags: Map[String, String] = Map.empty[String, String],
    startdate: Option[String] = None,
    enddate: Option[String] = None,
    dateFormat: String = this.dateFormat,
    conversionStrategy: ConversionStrategy = NoConversion
  ): RDD[DataPoint[_]] = {

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

    val metricScan = getMetricScan(
      tags,
      metricsUID.last,
      tagKUIDs,
      tagVUIDs,
      startdate,
      enddate,
      dateFormat
    )

    val rows = hbaseContext.hbaseRDD(TableName.valueOf(tsdbTable), metricScan).asInstanceOf[RDD[(ImmutableBytesWritable, Result)]]

    def process(row: (ImmutableBytesWritable, Result), tsdb: TSDB): Iterator[DataPoint[_]] = {
      val key = row._1.get()
      val metric = Internal.metricName(tsdb, key)
      val baseTime = Internal.baseTime(tsdb, key)
      val tags = Internal.getTags(tsdb, key).toMap

      var dps = new ListBuffer[DataPoint[_]]

      for (cell <- row._2.rawCells()) {
        val family = util.Arrays.copyOfRange(cell.getFamilyArray, cell.getFamilyOffset, cell.getFamilyOffset + cell.getFamilyLength)
        val qualifier = util.Arrays.copyOfRange(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierOffset + cell.getQualifierLength)
        val value = util.Arrays.copyOfRange(cell.getValueArray, cell.getValueOffset, cell.getValueOffset + cell.getValueLength)
        val kv = new KeyValue(key, family, qualifier, cell.getTimestamp, value)
        if (qualifier.length == 2 || qualifier.length == 4 && Internal.inMilliseconds(qualifier)) {
          val cell = Internal.parseSingleValue(kv)
          if (cell == null) {
            throw new IllegalDataException("Unable to parse row: " + kv)
          }
          dps += (conversionStrategy match {
            case ConvertToDouble => DataPoint(metric, tags, cell.absoluteTimestamp(baseTime), cell.parseValue().doubleValue())
            case NoConversion => if (cell.isInteger)
              DataPoint(metric, tags, cell.absoluteTimestamp(baseTime), cell.parseValue().longValue())
            else
              DataPoint(metric, tags, cell.absoluteTimestamp(baseTime), cell.parseValue().doubleValue())
          })
        } else {
          // compacted column
          val cells = new ListBuffer[Internal.Cell]
          try {
            cells ++= Internal.extractDataPoints(kv)
          } catch {
            case e: IllegalDataException =>
              throw new IllegalDataException(Bytes.toStringBinary(key), e);
          }
          for (cell <- cells) {
            dps += (conversionStrategy match {
              case ConvertToDouble => DataPoint(metric, tags, cell.absoluteTimestamp(baseTime), cell.parseValue().doubleValue())
              case NoConversion => if (cell.isInteger)
                DataPoint(metric, tags, cell.absoluteTimestamp(baseTime), cell.parseValue().longValue())
              else
                DataPoint(metric, tags, cell.absoluteTimestamp(baseTime), cell.parseValue().doubleValue())
            })
          }
        }
      }
      dps.iterator
    }

    val rdd = rows.mapPartitions[Iterator[DataPoint[_]]](iterator => {
      TSDBClientManager(keytab = keytab_, principal = principal_, hbaseContext = hbaseContext, tsdbTable = tsdbTable, tsdbUidTable = tsdbUidTable)
      new Iterator[Iterator[DataPoint[_]]] {
        val i = iterator.map(row => TSDBClientManager.tsdb.fold(throw _, process(row, _)))

        override def hasNext =
          if (!i.hasNext) {
            TSDBClientManager.shutdown()
            false
          } else
            i.hasNext

        override def next() = i.next()
      }
    })

    rdd.flatMap(identity[Iterator[DataPoint[_]]])
  }

  def write[T](timeseries: RDD[(String, Long, T, Map[String, String])])(implicit writeFunc: (Iterator[(String, Long, T, Map[String, String])], TSDB) => Unit): Unit = {
    timeseries.foreachPartition(it => {
      TSDBClientManager(keytab = keytab_, principal = principal_, hbaseContext = hbaseContext, tsdbTable = tsdbTable, tsdbUidTable = tsdbUidTable)
      TSDBClientManager.tsdb.fold(throw _, writeFunc(
        new Iterator[(String, Long, T, Map[String, String])] {
          override def hasNext =
            if (!it.hasNext) {
              TSDBClientManager.shutdown()
              false
            } else
              it.hasNext

          override def next() = it.next()
        }, _
      ))
    })
  }

  def streamWrite[T](dstream: DStream[(String, Long, T, Map[String, String])])(implicit writeFunc: (Iterator[(String, Long, T, Map[String, String])], TSDB) => Unit): Unit = {
    dstream foreachRDD {
      timeseries =>
        timeseries foreachPartition {
          it =>
            TSDBClientManager(keytab = keytab_, principal = principal_, hbaseContext = hbaseContext, tsdbTable = tsdbTable, tsdbUidTable = tsdbUidTable)
            TSDBClientManager.tsdb.fold(throw _, writeFunc(
              new Iterator[(String, Long, T, Map[String, String])] {
                override def hasNext =
                  if (!it.hasNext) {
                    TSDBClientManager.shutdown()
                    false
                  } else
                    it.hasNext

                override def next() = it.next()
              }, _
            ))
        }
    }
  }

}
