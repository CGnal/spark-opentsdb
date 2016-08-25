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

import java.io.{ BufferedWriter, File, FileWriter }
import java.nio.ByteBuffer
import java.nio.file.{ Files, Paths }
import java.sql.Timestamp
import java.util.{ Calendar, TimeZone }

import cats.data.Xor
import net.opentsdb.core.TSDB
import net.opentsdb.utils.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{ RegexStringComparator, RowFilter }
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import shaded.org.hbase.async.HBaseClient

import scala.language.implicitConversions

package object opentsdb {

  implicit val writeForByte: (Iterator[DataPoint[Byte]], TSDB) => Unit = (it, tsdb) => {
    import collection.JavaConversions._
    it.foreach(dp => {
      tsdb.addPoint(dp.metric, dp.timestamp, dp.value.asInstanceOf[Long], dp.tags)
    })
  }

  implicit val writeForShort: (Iterator[DataPoint[Short]], TSDB) => Unit = (it, tsdb) => {
    import collection.JavaConversions._
    it.foreach(dp => {
      tsdb.addPoint(dp.metric, dp.timestamp, dp.value.asInstanceOf[Long], dp.tags)
    })
  }

  implicit val writeForInt: (Iterator[DataPoint[Int]], TSDB) => Unit = (it, tsdb) => {
    import collection.JavaConversions._
    it.foreach(dp => {
      tsdb.addPoint(dp.metric, dp.timestamp, dp.value.asInstanceOf[Long], dp.tags)
    })
  }

  implicit val writeForLong: (Iterator[DataPoint[Long]], TSDB) => Unit = (it, tsdb) => {
    import collection.JavaConversions._
    it.foreach(dp => {
      tsdb.addPoint(dp.metric, dp.timestamp, dp.value, dp.tags)
    })
  }

  implicit val writeForFloat: (Iterator[DataPoint[Float]], TSDB) => Unit = (it, tsdb) => {
    import collection.JavaConversions._
    it.foreach(dp => {
      tsdb.addPoint(dp.metric, dp.timestamp, dp.value, dp.tags)
    })
  }

  implicit val writeForDouble: (Iterator[DataPoint[Double]], TSDB) => Unit = (it, tsdb) => {
    import collection.JavaConversions._
    it.foreach(dp => {
      tsdb.addPoint(dp.metric, dp.timestamp, dp.value, dp.tags)
    })
  }

  @inline implicit def timestampWrapper(ts: Timestamp) = new RichTimestamp(ts)

  object TSDBClientManager {

    @transient lazy val log = Logger.getLogger(getClass.getName)

    @inline private def writeStringToFile(file: File, str: String): Unit = {
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write(str)
      bw.close()
    }

    @inline private def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    def shutdown() = {
      log.trace("About to shutdown the TSDB client instance")
      _tsdb.foreach(_.fold(throw _, _.shutdown().joinUninterruptibly()))
      _tsdb = None
    }

    var _tsdb: Option[Throwable Xor TSDB] = None

    var _config: Option[Config] = None

    var _asyncConfig: Option[shaded.org.hbase.async.Config] = None

    def tsdb: Throwable Xor TSDB = try {
      _tsdb.getOrElse {
        try {
          log.trace("Creating the TSDB client instance")
          val hbaseClient = new HBaseClient(_asyncConfig.getOrElse(throw new Exception("no configuration available")))
          val tsdb = Xor.right[Throwable, TSDB](new TSDB(hbaseClient, _config.getOrElse(throw new Exception("no configuration available"))))
          _tsdb = Some(tsdb)
          tsdb
        } catch {
          case e: Throwable => Xor.left[Throwable, TSDB](e)
        }
      }
    }

    def apply(
      keytab: Option[Broadcast[Array[Byte]]],
      principal: Option[String],
      hbaseContext: HBaseContext,
      tsdbTable: String,
      tsdbUidTable: String
    ): Unit = {
      val configuration: Configuration = {
        val configuration: Configuration = hbaseContext.broadcastedConf.value.value
        val authenticationType = configuration.get("hbase.security.authentication")
        if (authenticationType == null)
          HBaseConfiguration.create()
        else
          configuration
      }
      val authenticationType = configuration.get("hbase.security.authentication")
      val quorum = configuration.get("hbase.zookeeper.quorum")
      val port = configuration.get("hbase.zookeeper.property.clientPort")
      val asyncConfig = new shaded.org.hbase.async.Config()
      val config = new Config(false)
      config.overrideConfig("tsd.storage.hbase.data_table", tsdbTable)
      config.overrideConfig("tsd.storage.hbase.uid_table", tsdbUidTable)
      config.overrideConfig("tsd.core.auto_create_metrics", "true")
      config.disableCompactions()
      asyncConfig.overrideConfig("hbase.zookeeper.quorum", s"$quorum:$port")
      asyncConfig.overrideConfig("hbase.zookeeper.znode.parent", "/hbase")
      if (authenticationType == "kerberos") {
        val keytabPath = s"$getCurrentDirectory/keytab"
        val byteArray = keytab.getOrElse(throw new Exception("keytab data not available")).value
        Files.write(Paths.get(keytabPath), byteArray)
        val jaasFile = java.io.File.createTempFile("jaas", ".jaas")
        val jaasConf =
          s"""AsynchbaseClient {
              |  com.sun.security.auth.module.Krb5LoginModule required
              |  useTicketCache=false
              |  useKeyTab=true
              |  keyTab="$keytabPath"
              |  principal="${principal.getOrElse(throw new Exception("principal not available"))}"
              |  storeKey=true;
                };
            """.stripMargin
        writeStringToFile(jaasFile, jaasConf)
        System.setProperty(
          "java.security.auth.login.config",
          jaasFile.getAbsolutePath
        )
        configuration.set("hadoop.security.authentication", "kerberos")
        asyncConfig.overrideConfig("hbase.security.auth.enable", "true")
        asyncConfig.overrideConfig("hbase.security.authentication", "kerberos")
        asyncConfig.overrideConfig("hbase.kerberos.regionserver.principal", configuration.get("hbase.regionserver.kerberos.principal"))
        asyncConfig.overrideConfig("hbase.sasl.clientconfig", "AsynchbaseClient")
        asyncConfig.overrideConfig("hbase.rpc.protection", configuration.get("hbase.rpc.protection"))
        log.trace("Created kerberos configuration environment")
        log.trace(s"principal: ${principal.getOrElse(throw new Exception)}")
        log.trace(s"jaas path: ${jaasFile.getAbsolutePath}")
        log.trace(s"keytab path: $keytabPath")
      }
      _config = Some(config)
      _asyncConfig = Some(asyncConfig)
    }

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
    val name = if (tagKV.nonEmpty)
      String.format("^%s.*%s.*$", bytes2hex(metricUID, "\\x"), bytes2hex(tagKV.flatten.toArray, "\\x"))
    else
      String.format("^%s.+$", bytes2hex(metricUID, "\\x"))

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

    if (tagKV.nonEmpty) {
      scan.setStartRow(hexStringToByteArray(bytes2hex(metricUID, "\\x") + bytes2hex(stDateBuffer.array(), "\\x") + bytes2hex(tagKV.flatten.toArray, "\\x")))
      scan.setStopRow(hexStringToByteArray(bytes2hex(metricUID, "\\x") + bytes2hex(endDateBuffer.array(), "\\x") + bytes2hex(tagKV.flatten.toArray, "\\x")))
    } else {
      scan.setStartRow(hexStringToByteArray(bytes2hex(metricUID, "\\x") + bytes2hex(stDateBuffer.array(), "\\x")))
      scan.setStopRow(hexStringToByteArray(bytes2hex(metricUID, "\\x") + bytes2hex(endDateBuffer.array(), "\\x")))
    }
    scan
  }

  private def bytes2hex(bytes: Array[Byte], sep: String): String = {
    sep + bytes.map("%02x".format(_)).mkString(sep)
  }

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
    def opentsdb(): DataFrame = reader.format("com.cgnal.spark.opentsdb").load
  }

  implicit class KuduDataFrameWriter(writer: DataFrameWriter) {
    def opentsdb() = writer.format("com.cgnal.spark.opentsdb").save
  }

  implicit class rddWrapper(rdd: RDD[DataPoint[Double]]) {
    val schema = StructType(
      Array(
        StructField("timestamp", TimestampType, nullable = false),
        StructField("metric", StringType, nullable = false),
        StructField("value", DoubleType, nullable = false),
        StructField("tags", DataTypes.createMapType(StringType, StringType), nullable = false)
      )
    )

    def toDF(implicit sqlContext: SQLContext) = {
      val df = rdd.map {
        dp =>
          Row(new Timestamp(dp.timestamp), dp.metric, dp.value, dp.tags)
      }
      sqlContext.createDataFrame(df, schema)
    }
  }

}
