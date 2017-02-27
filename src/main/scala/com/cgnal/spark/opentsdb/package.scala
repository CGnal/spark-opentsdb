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

package com.cgnal.spark

import java.nio.ByteBuffer
import java.sql.Timestamp
import java.util.{ Calendar, TimeZone }

import com.stumbleupon.async.{ Callback, Deferred }
import net.opentsdb.core.{ TSDB, WritableDataPoints }
import org.apache.hadoop.hbase.client.{ Result, Scan }
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{ RegexStringComparator, RowFilter }
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{ IdentityTableMapper, TableInputFormat, TableMapReduceUtil }
import org.apache.hadoop.hbase.{ HBaseConfiguration, TableName }
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.security.UserGroupInformation
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import shaded.org.hbase.async.{ HBaseRpc, PleaseThrottleException, PutRequest }

import scala.collection.convert.decorateAsJava._
import scala.collection.mutable
import scala.language.implicitConversions

package object opentsdb {

  @transient private lazy val log = Logger.getLogger(getClass.getName)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def doThrottle(d: Deferred[AnyRef]) = {
    log.info("Throttling...")
    var throttle_time = System.nanoTime()
    try {
      d.joinUninterruptibly()
    } catch {
      case e: Exception =>
        throw new RuntimeException("Should never happen", e)
    }
    throttle_time = System.nanoTime() - throttle_time
    if (throttle_time < 1000000000L) {
      log.info("Got throttled for only " + throttle_time + "ns, sleeping a bit now")
      try {
        Thread.sleep(1000)
      } catch {
        case e: InterruptedException =>
          throw new RuntimeException("interrupted", e)
      }
    }
    log.info("Done throttling...")
  }

  @SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures", "org.wartremover.warts.Var"))
  @inline private def addLongDataPoints[T <: AnyVal](it: Iterator[DataPoint[T]], tsdb: TSDB) = {
    var datapoints = mutable.Map.empty[String, WritableDataPoints]
    @volatile var throttle = false
    val cb = new Callback[Unit, AnyRef] {
      def call(arg: AnyRef): Unit = {
        arg match {
          case ex: PleaseThrottleException =>
            log.warn("Need to throttle, HBase isn't keeping up.", ex)
            throttle = true
            val rpc: HBaseRpc = ex.getFailedRpc
            rpc match {
              case op: PutRequest =>
                tsdb.getClient.put(op)
            }
          case ex: Exception =>
            log.error("Failing in writing a datapoint", ex)
        }
        ()
      }
    }
    it.foreach(dp => {
      val d = tsdb.addPoint(dp.metric, dp.timestamp, dp.value.asInstanceOf[Long], dp.tags.asJava)
      d.addBoth(cb)
      if (throttle) {
        doThrottle(d)
        throttle = false
      }
    })
  }

  @SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures", "org.wartremover.warts.Var"))
  @inline private def addFloatDataPoints[T <: AnyVal](it: Iterator[DataPoint[T]], tsdb: TSDB) = {
    @volatile var throttle = false
    val cb = new Callback[Unit, AnyRef] {
      def call(arg: AnyRef): Unit = {
        arg match {
          case ex: PleaseThrottleException =>
            log.warn("Need to throttle, HBase isn't keeping up.", ex)
            throttle = true
            val rpc: HBaseRpc = ex.getFailedRpc
            rpc match {
              case op: PutRequest =>
                tsdb.getClient.put(op)
            }
          case ex: Exception =>
            log.error("Failing in writing a datapoint", ex)
        }
        ()
      }
    }
    it.foreach(dp => {
      val d = tsdb.addPoint(dp.metric, dp.timestamp, dp.value.asInstanceOf[Float], dp.tags.asJava)
      d.addBoth(cb)
      if (throttle) {
        doThrottle(d)
        throttle = false
      }
    })
  }

  @SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures", "org.wartremover.warts.Var"))
  @inline private def addDoubleDataPoints[T <: AnyVal](it: Iterator[DataPoint[T]], tsdb: TSDB) = {
    @volatile var throttle = false
    val cb = new Callback[Unit, AnyRef] {
      def call(arg: AnyRef): Unit = {
        arg match {
          case ex: PleaseThrottleException =>
            log.warn("Need to throttle, HBase isn't keeping up.", ex)
            throttle = true
            val rpc: HBaseRpc = ex.getFailedRpc
            rpc match {
              case op: PutRequest =>
                tsdb.getClient.put(op)
            }
          case ex: Exception =>
            log.error("Failing in writing a datapoint", ex)
        }
        ()
      }
    }
    it.foreach(dp => {
      val d = tsdb.addPoint(dp.metric, dp.timestamp, dp.value.asInstanceOf[Double], dp.tags.asJava)
      d.addBoth(cb)
      if (throttle) {
        doThrottle(d)
        throttle = false
      }
    })
  }

  implicit val writeForByte: (Iterator[DataPoint[Byte]], TSDB) => Unit = (it, tsdb) => {
    addLongDataPoints(it, tsdb)
  }

  implicit val writeForShort: (Iterator[DataPoint[Short]], TSDB) => Unit = (it, tsdb) => {
    addLongDataPoints(it, tsdb)
  }

  implicit val writeForInt: (Iterator[DataPoint[Int]], TSDB) => Unit = (it, tsdb) => {
    addLongDataPoints(it, tsdb)
  }

  implicit val writeForLong: (Iterator[DataPoint[Long]], TSDB) => Unit = (it, tsdb) => {
    addLongDataPoints(it, tsdb)
  }

  implicit val writeForFloat: (Iterator[DataPoint[Float]], TSDB) => Unit = (it, tsdb) => {
    addFloatDataPoints(it, tsdb)
  }

  implicit val writeForDouble: (Iterator[DataPoint[Double]], TSDB) => Unit = (it, tsdb) => {
    addDoubleDataPoints(it, tsdb)
  }

  private[opentsdb] def getUIDScan(metricName: String, tags: Map[String, String]) = {
    val scan = new Scan()
    val name: String = String.format("^(%s)$", Array(metricName, tags.keys.mkString("|"), tags.values.mkString("|")).mkString("|"))
    val keyRegEx: RegexStringComparator = new RegexStringComparator(name)
    val rowFilter: RowFilter = new RowFilter(CompareOp.EQUAL, keyRegEx)
    scan.setFilter(rowFilter)
    scan
  }

  private[opentsdb] def getMetricScan(
    bucket: Byte,
    tags: Map[String, String],
    metricUID: Array[Byte],
    tagKUIDs: Map[String, Array[Byte]],
    tagVUIDs: Map[String, Array[Byte]],
    interval: Option[(Long, Long)]
  ) = {
    val tagKKeys = tagKUIDs.keys.toArray
    val tagVKeys = tagVUIDs.keys.toArray
    val ntags = tags.filter(kv => tagKKeys.contains(kv._1) && tagVKeys.contains(kv._2))
    val tagKV = tagKUIDs.
      filter(kv => ntags.contains(kv._1)).
      map(k => (k._2, tagVUIDs(tags(k._1)))).
      map(l => l._1 ++ l._2).toList.sorted(Ordering.by((_: Array[Byte]).toIterable))
    val scan = new Scan()
    val name = if (bucket >= 0) {
      if (tagKV.nonEmpty)
        String.format("^%s%s.*%s.*$", bytes2hex(Array(bucket), "\\x"), bytes2hex(metricUID, "\\x"), bytes2hex(tagKV.flatten.toArray, "\\x"))
      else
        String.format("^%s%s.+$", bytes2hex(Array(bucket), "\\x"), bytes2hex(metricUID, "\\x"))
    } else {
      if (tagKV.nonEmpty)
        String.format("^%s.*%s.*$", bytes2hex(metricUID, "\\x"), bytes2hex(tagKV.flatten.toArray, "\\x"))
      else
        String.format("^%s.+$", bytes2hex(metricUID, "\\x"))
    }
    val keyRegEx: RegexStringComparator = new RegexStringComparator(name)
    val rowFilter: RowFilter = new RowFilter(CompareOp.EQUAL, keyRegEx)
    scan.setFilter(rowFilter)

    val minDate = (new Calendar.Builder().setTimeZone(TimeZone.getTimeZone("UTC")).setDate(1970, 0, 1).setTimeOfDay(0, 0, 0).build().getTime.getTime / 1000).toInt
    val maxDate = (new Calendar.Builder().setTimeZone(TimeZone.getTimeZone("UTC")).setDate(2099, 11, 31).setTimeOfDay(23, 59, 0).build().getTime.getTime / 1000).toInt

    val stDateBuffer = ByteBuffer.allocate(4)
    val endDateBuffer = ByteBuffer.allocate(4)

    interval.fold({
      stDateBuffer.putInt(minDate)
      endDateBuffer.putInt(maxDate)
    })(interval => {
      stDateBuffer.putInt(interval._1.toInt)
      endDateBuffer.putInt(interval._2.toInt)
    })
    if (bucket >= 0) {
      if (tagKV.nonEmpty) {
        scan.setStartRow(hexStringToByteArray(bytes2hex(Array(bucket), "\\x") + bytes2hex(metricUID, "\\x") + bytes2hex(stDateBuffer.array(), "\\x") + bytes2hex(tagKV.flatten.toArray, "\\x")))
        scan.setStopRow(hexStringToByteArray(bytes2hex(Array(bucket), "\\x") + bytes2hex(metricUID, "\\x") + bytes2hex(endDateBuffer.array(), "\\x") + bytes2hex(tagKV.flatten.toArray, "\\x")))
      } else {
        scan.setStartRow(hexStringToByteArray(bytes2hex(Array(bucket), "\\x") + bytes2hex(metricUID, "\\x") + bytes2hex(stDateBuffer.array(), "\\x")))
        scan.setStopRow(hexStringToByteArray(bytes2hex(Array(bucket), "\\x") + bytes2hex(metricUID, "\\x") + bytes2hex(endDateBuffer.array(), "\\x")))
      }
    } else {
      if (tagKV.nonEmpty) {
        scan.setStartRow(hexStringToByteArray(bytes2hex(metricUID, "\\x") + bytes2hex(stDateBuffer.array(), "\\x") + bytes2hex(tagKV.flatten.toArray, "\\x")))
        scan.setStopRow(hexStringToByteArray(bytes2hex(metricUID, "\\x") + bytes2hex(endDateBuffer.array(), "\\x") + bytes2hex(tagKV.flatten.toArray, "\\x")))
      } else {
        scan.setStartRow(hexStringToByteArray(bytes2hex(metricUID, "\\x") + bytes2hex(stDateBuffer.array(), "\\x")))
        scan.setStopRow(hexStringToByteArray(bytes2hex(metricUID, "\\x") + bytes2hex(endDateBuffer.array(), "\\x")))
      }
    }
    scan
  }

  private def bytes2hex(bytes: Array[Byte], sep: String): String = {
    sep + bytes.map("%02x".format(_)).mkString(sep)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.While"))
  private def hexStringToByteArray(s: String): Array[Byte] = {
    val sn = s.replace("\\x", "")
    val b: Array[Byte] = new Array[Byte](sn.length / 2)
    var i: Int = 0
    while (i < b.length) {
      {
        val index: Int = i * 2
        val v: Int = Integer.parseInt(sn.substring(index, index + 2), 16)
        b(i) = v.toByte
      }
      {
        i += 1
        i - 1
      }
    }
    b
  }

  implicit class OpenTSDBDataFrameReader(reader: DataFrameReader) {
    def opentsdb: DataFrame = reader.format("com.cgnal.spark.opentsdb").load
  }

  implicit class OpenTSDBDataFrameWriter(writer: DataFrameWriter[Row]) {
    def opentsdb(): Unit = writer.format("com.cgnal.spark.opentsdb").save
  }

  implicit class rddWrapper(rdd: RDD[DataPoint[Double]]) {

    def toDF(implicit sparkSession: SparkSession): DataFrame = {
      val df = rdd.map {
        dp =>
          Row(new Timestamp(dp.timestamp), dp.metric, dp.value, dp.tags)
      }
      sparkSession.createDataFrame(df, StructType(
        Array(
          StructField("timestamp", TimestampType, nullable = false),
          StructField("metric", StringType, nullable = false),
          StructField("value", DoubleType, nullable = false),
          StructField("tags", DataTypes.createMapType(StringType, StringType), nullable = false)
        )
      ))
    }
  }

  implicit class RichTimestamp(val self: Timestamp) extends AnyVal {
    def -->(end: Timestamp): (Long, Long) = (
      self.getTime / 1000,
      end.getTime / 1000
    )
  }

  implicit class EnrichedSparkSession(sparkSession: SparkSession) {

    def loadTable(tableName: TableName, scan: Scan): RDD[(ImmutableBytesWritable, Result)] = {
      val conf = new JobConf(HBaseConfiguration.create(sparkSession.sparkContext.hadoopConfiguration))
      val job = Job.getInstance(conf)
      TableMapReduceUtil.initTableMapperJob(tableName, scan, classOf[IdentityTableMapper], null, null, job)
      conf.getCredentials.addAll {
        UserGroupInformation.getCurrentUser.getCredentials
      }
      sparkSession.sparkContext.newAPIHadoopRDD(
        job.getConfiguration,
        classOf[TableInputFormat],
        classOf[ImmutableBytesWritable],
        classOf[Result]
      )
    }

    def loadTable(tableName: String, scan: Scan): RDD[(ImmutableBytesWritable, Result)] = loadTable(TableName.valueOf(tableName), scan)

  }

}
