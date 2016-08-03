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
import java.text.SimpleDateFormat
import java.time.Instant
import java.util.TimeZone

import com.cloudera.sparkts.MillisecondFrequency
import net.opentsdb.core.TSDB
import net.opentsdb.tools.FileImporter
import net.opentsdb.utils.Config
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types._
import org.hbase.async.HBaseClient

import scala.collection.JavaConversions._
import scala.collection.mutable

class SparkSpec extends SparkBaseSpec {

  "Spark" must {
    "load a timeseries from OpenTSDB correctly" in {

      for (i <- 0 until 10) {
        val ts = Timestamp.from(Instant.parse(s"2016-07-05T${10 + i}:00:00.00Z"))
        val epoch = ts.getTime
        tsdb.addPoint("mymetric", epoch, i.toLong, Map("key1" -> "value1", "key2" -> "value2")).joinUninterruptibly()
      }

      for (i <- 0 until 10) {
        val ts = Timestamp.from(Instant.parse(s"2016-07-06T${10 + i}:00:00.00Z"))
        val epoch = ts.getTime
        tsdb.addPoint("mymetric", epoch, (i + 100).toLong, Map("key1" -> "value1", "key3" -> "value3")).joinUninterruptibly()
      }

      // Default Date Format: dd/MM/yyyy HH:mm
      {
        val simpleDateFormat = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm")
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
        val ts = openTSDBContext.load("mymetric", Map("key1" -> "value1", "key2" -> "value2"), Some("05/07/2016 10:00"), Some("05/07/2016 20:00"))

        val result = ts.collect()

        result.length must be(10)

        result.foreach(p => println((simpleDateFormat.format(p.getAs[Timestamp](0)), p.getAs[Long](2))))
      }
      println("------------")

      {
        val simpleDateFormat = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm")
        val ts = openTSDBContext.load("mymetric", Map("key1" -> "value1"), Some("05/07/2016 10:00"), Some("06/07/2016 20:00"))

        val result = ts.collect()

        result.length must be(20)

        result.foreach(p => println((simpleDateFormat.format(p.getAs[Timestamp](0)), p.getAs[Long](2))))
      }
      println("------------")

      {
        val simpleDateFormat = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm")
        val ts = openTSDBContext.load("mymetric", Map("key1" -> "value1", "key3" -> "value3"), Some("05/07/2016 10:00"), Some("06/07/2016 20:00"))

        val result = ts.collect()

        result.length must be(10)

        result.foreach(p => println((simpleDateFormat.format(p.getAs[Timestamp](0)), p.getAs[Long](2))))
      }
      println("------------")

      {
        val simpleDateFormat = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm")
        val ts = openTSDBContext.load("mymetric", Map("key1" -> "value1", "key2" -> "value2"), Some("05/07/2016 10:00"), Some("06/07/2016 20:00"))

        val result = ts.collect()

        result.length must be(10)

        result.foreach(p => println((simpleDateFormat.format(p.getAs[Timestamp](0)), p.getAs[Long](2))))
      }
      println("------------")

      {
        val simpleDateFormat = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm")
        val ts = openTSDBContext.load("mymetric")

        val result = ts.collect()

        result.length must be(20)

        result.foreach(p => println((simpleDateFormat.format(p.getAs[Timestamp](0)), p.getAs[Long](2))))
      }
    }
  }

  "Spark" must {
    "load a timeseries with milliseconds granularity correctly" in {
      for (i <- 0 until 10)
        tsdb.addPoint("anothermetric", i.toLong, (i - 10).toFloat, Map("key1" -> "value1", "key2" -> "value2")).joinUninterruptibly()

      val ts = openTSDBContext.load("anothermetric", Map("key1" -> "value1", "key2" -> "value2"), None, None, conversionStrategy = ConvertToDouble)

      val result = ts.collect()

      result.map(r => (r.getAs[Timestamp](0).getTime, r.getAs[Double](2))) must be((0 until 10).map(i => (i.toLong, (i - 10).toDouble)))
    }
  }

  "Spark" must {
    "load a timeseries dataframe from OpenTSDB specifying only the metric name correctly" in {

      val configuration: Configuration = hbaseContext.broadcastedConf.value.value
      val quorum = configuration.get("hbase.zookeeper.quorum")
      val port = configuration.get("hbase.zookeeper.property.clientPort")
      val config = new Config(false)
      config.overrideConfig("tsd.storage.hbase.data_table", openTSDBContext.tsdbTable)
      config.overrideConfig("tsd.storage.hbase.uid_table", openTSDBContext.tsdbUidTable)
      config.overrideConfig("tsd.core.auto_create_metrics", "true")
      val hBaseClient = new HBaseClient(s"$quorum:$port")
      val tsdb = new TSDB(hBaseClient, config)

      FileImporter.importFile(hBaseClient, tsdb, "data/opentsdb.input", skip_errors = false)

      val df = openTSDBContext.loadDataFrame(sqlContext, "open", Map.empty[String, String], Some("06/06/2016 20:00"), Some("27/06/2016 17:00"))

      df.registerTempTable("open")

      val out = sqlContext.sql("select tags['symbol'], timestamp, value from open where tags['symbol'] = 'AAPL' sort by(timestamp)")

      val values1 = out.map(_.getAs[Double](2)).take(5625) //.collect() //TODO this test fails with collect the, is it a consequence of running inside a test?

      val rdd = sparkContext.textFile("data/opentsdb.input")

      val splittedLines = rdd.map {
        line =>
          line.split(' ')
      }

      val values2 = splittedLines.
        filter(splittedLine => splittedLine(0) == "open" && splittedLine(3) == "symbol=AAPL").
        map(splittedLine => (splittedLine(1).toLong, splittedLine(2).toDouble)).map(_._2).collect()

      values1.length must be(values2.length)

      for (i <- 0 until values1.length) {
        val diff = Math.abs(values1(i) - values2(i))
        println(s"$i ${values1(i)} ${values2(i)} $diff")
        diff < 0.00001 must be(true)
      }
    }
  }

  "Spark" must {
    "load timeseries from OpenTSDB into a Spark Timeseries RDD correctly" in {

      for (i <- 0 until 1000) {
        tsdb.addPoint("metric1", i.toLong, (i - 10).toFloat, Map("key1" -> "value1"))
        tsdb.addPoint("metric2", i.toLong, (i - 20).toFloat, Map("key1" -> "value1"))
        tsdb.addPoint("metric3", i.toLong, (i - 30).toFloat, Map("key1" -> "value1"))
        tsdb.addPoint("metric4", i.toLong, (i - 40).toFloat, Map("key1" -> "value1"))
        tsdb.addPoint("metric5", i.toLong, (i - 50).toFloat, Map("key1" -> "value1"))
      }

      val startDate = Timestamp.from(Instant.parse(s"1970-01-01T00:00:00.000Z"))
      val endDate = Timestamp.from(Instant.parse(s"1970-01-01T00:00:01.00Z"))
      val simpleDateFormat = new SimpleDateFormat("yyyy-MM-ss mm-ss-SSS")
      simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))

      val ts = openTSDBContext.loadTimeSeriesRDD(
        sqlContext,
        simpleDateFormat.format(startDate),
        simpleDateFormat.format(endDate),
        new MillisecondFrequency(1),
        List(
          "metric1" -> Map("key1" -> "value1"),
          "metric2" -> Map("key1" -> "value1"),
          "metric3" -> Map("key1" -> "value1"),
          "metric4" -> Map("key1" -> "value1"),
          "metric5" -> Map("key1" -> "value1")
        ),
        "yyyy-MM-ss mm-ss-SSS"
      )

      ts.findSeries("metric1").asInstanceOf[org.apache.spark.mllib.linalg.Vector].size must be(1000)
      ts.findSeries("metric2").asInstanceOf[org.apache.spark.mllib.linalg.Vector].size must be(1000)
      ts.findSeries("metric3").asInstanceOf[org.apache.spark.mllib.linalg.Vector].size must be(1000)
      ts.findSeries("metric4").asInstanceOf[org.apache.spark.mllib.linalg.Vector].size must be(1000)
      ts.findSeries("metric5").asInstanceOf[org.apache.spark.mllib.linalg.Vector].size must be(1000)

    }
  }

  "Spark" must {
    "load a timeseries dataframe from OpenTSDB correctly" in {

      for (i <- 0 until 10) {
        val ts = Timestamp.from(Instant.parse(s"2016-07-05T${10 + i}:00:00.00Z"))
        val epoch = ts.getTime
        tsdb.addPoint("mymetric", epoch, i.toLong, Map("key1" -> "value1", "key2" -> "value2")).joinUninterruptibly()
      }

      val simpleDateFormat = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm")
      simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
      val df = openTSDBContext.loadDataFrame(sqlContext, "mymetric", Map("key1" -> "value1", "key2" -> "value2"), Some("05/07/2016 10:00"), Some("05/07/2016 20:00"), conversionStrategy = ConvertToFloat)

      df.schema must be(
        StructType(
          Array(
            StructField("timestamp", TimestampType, nullable = false),
            StructField("metric", StringType, nullable = false),
            StructField("value", FloatType, nullable = false),
            StructField("tags", DataTypes.createMapType(StringType, StringType), nullable = false)
          )
        )
      )

      val result = df.collect()

      result.length must be(10)

      result.foreach(println(_))

    }
  }

  "Spark" must {
    "save timeseries points from a Spark stream correctly" in {

      val points = for {
        i <- 0 until 10
        ts = Timestamp.from(Instant.parse(s"2016-07-05T${10 + i}:00:00.00Z"))
        epoch = ts.getTime
        point = ("mymetric1", epoch, i.toDouble, Map("key1" -> "value1", "key2" -> "value2"))
      } yield point

      val rdd = sparkContext.parallelize[(String, Long, Double, Map[String, String])](points)

      val stream = streamingContext.queueStream[(String, Long, Double, Map[String, String])](mutable.Queue(rdd))

      openTSDBContext.streamWrite(stream)

      streamingContext.start()

      Thread.sleep(1000)

      val simpleDateFormat = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm")
      simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
      val df = openTSDBContext.loadDataFrame(sqlContext, "mymetric1", Map("key1" -> "value1", "key2" -> "value2"), Some("05/07/2016 10:00"), Some("05/07/2016 20:00"), conversionStrategy = ConvertToFloat)

      df.schema must be(
        StructType(
          Array(
            StructField("timestamp", TimestampType, nullable = false),
            StructField("metric", StringType, nullable = false),
            StructField("value", FloatType, nullable = false),
            StructField("tags", DataTypes.createMapType(StringType, StringType), nullable = false)
          )
        )
      )

      val result = df.collect()

      result.length must be(10)

      result.foreach(println(_))
    }
  }

}