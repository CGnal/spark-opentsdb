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
import java.time.Instant
import java.util.TimeZone

import net.opentsdb.tools.FileImporter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.types._

import scala.collection.convert.decorateAsJava._
import scala.collection.mutable

class SparkSpec extends SparkBaseSpec {

  "Spark" must {
    "load a timeseries from OpenTSDB correctly" in {

      for (i <- 0 until 10) {
        val ts = Timestamp.from(Instant.parse(s"2016-07-05T${10 + i}:00:00.00Z"))
        val epoch = ts.getTime
        tsdb.addPoint("metric", epoch, i.toLong, Map("key1" -> "value1", "key2" -> "value2").asJava).joinUninterruptibly()
      }

      for (i <- 0 until 10) {
        val ts = Timestamp.from(Instant.parse(s"2016-07-06T${10 + i}:00:00.00Z"))
        val epoch = ts.getTime
        tsdb.addPoint("metric", epoch, (i + 100).toLong, Map("key1" -> "value1", "key3" -> "value3").asJava).joinUninterruptibly()
      }

      {
        val simpleDateFormat = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm")
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))

        val tsStart = Timestamp.from(Instant.parse(s"2016-07-05T10:00:00.00Z"))
        val tsEnd = Timestamp.from(Instant.parse(s"2016-07-05T20:00:00.00Z"))

        val ts = openTSDBContext.load("metric", Map("key1" -> "value1", "key2" -> "value2"), Some(tsStart --> tsEnd))

        val result = ts.collect()

        result.length must be(10)

        result.foreach(dp => println((simpleDateFormat.format(new Timestamp(dp.timestamp)), dp.value)))
      }
      println("------------")

      {
        val simpleDateFormat = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm")
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))

        val tsStart = Timestamp.from(Instant.parse(s"2016-07-05T10:00:00.00Z"))
        val tsEnd = Timestamp.from(Instant.parse(s"2016-07-06T20:00:00.00Z"))

        val ts = openTSDBContext.load("metric", Map("key1" -> "value1"), Some(tsStart --> tsEnd))

        val result = ts.collect()

        result.length must be(20)

        result.foreach(dp => println((simpleDateFormat.format(new Timestamp(dp.timestamp)), dp.value)))
      }
      println("------------")

      {
        val simpleDateFormat = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm")
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))

        val tsStart = Timestamp.from(Instant.parse(s"2016-07-05T10:00:00.00Z"))
        val tsEnd = Timestamp.from(Instant.parse(s"2016-07-06T20:00:00.00Z"))

        val ts = openTSDBContext.load("metric", Map("key1" -> "value1", "key3" -> "value3"), Some(tsStart --> tsEnd))

        val result = ts.collect()

        result.length must be(10)

        result.foreach(dp => println((simpleDateFormat.format(new Timestamp(dp.timestamp)), dp.value)))
      }
      println("------------")

      {
        val simpleDateFormat = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm")
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))

        val tsStart = Timestamp.from(Instant.parse(s"2016-07-05T10:00:00.00Z"))
        val tsEnd = Timestamp.from(Instant.parse(s"2016-07-05T20:00:00.00Z"))

        val ts = openTSDBContext.load("metric", Map("key1" -> "value1", "key2" -> "value2"), Some(tsStart --> tsEnd))

        val result = ts.collect()

        result.length must be(10)

        result.foreach(dp => println((simpleDateFormat.format(new Timestamp(dp.timestamp)), dp.value)))
      }
      println("------------")

      {
        val simpleDateFormat = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm")
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))

        val ts = openTSDBContext.load("metric")

        val result = ts.collect()

        result.length must be(20)

        result.foreach(dp => println((simpleDateFormat.format(new Timestamp(dp.timestamp)), dp.value)))
      }
    }
  }

  "Spark" must {
    "load a timeseries with milliseconds granularity correctly" in {
      val tsStart = Timestamp.from(Instant.parse(s"2016-07-05T10:00:00.00Z")).getTime
      val tsEnd = tsStart + 2000

      for (i <- tsStart until tsEnd)
        tsdb.addPoint("anothermetric", i.toLong, (i - tsStart).toDouble, Map("key1" -> "value1", "key2" -> "value2").asJava)

      val ts = openTSDBContext.load("anothermetric", Map("key1" -> "value1", "key2" -> "value2"), Some((tsStart / 1000, tsStart / 1000 + 1)), conversionStrategy = ConvertToDouble)

      val result = ts.collect()

      result.length must be(1000)

      result.map(dp => (dp.timestamp, dp.value)) must be((tsStart until tsStart + 1000).map(i => (i, (i - tsStart).toDouble)))
    }
  }

  "Spark" must {
    "load a timeseries dataframe from OpenTSDB specifying only the metric name correctly" in {

      FileImporter.importFile(hbaseAsyncClient, tsdb, "data/opentsdb.input", skip_errors = true)

      val tsStart = Timestamp.from(Instant.parse(s"2016-06-06T20:00:00.00Z"))
      val tsEnd = Timestamp.from(Instant.parse(s"2016-06-27T17:00:00.00Z"))

      val df = openTSDBContext.loadDataFrame("open", Map.empty[String, String], Some(tsStart --> tsEnd))

      df.createOrReplaceTempView("open")

      val ss = sparkSession
      import ss.implicits._
      val out = sparkSession.sql("select tags['symbol'], timestamp, value from open where tags['symbol'] = 'AAPL'")

      val values1 = out.map(_.getAs[Double](2)).take(5625) //.collect() //TODO this test fails with collect the, is it a consequence of running inside a test?

      val mapredWorkingDir = new Path(sparkSession.sparkContext.hadoopConfiguration.get("mapred.working.dir"))

      val dataDirPath = new Path(mapredWorkingDir, "data")
      val dataFilePath = new Path(dataDirPath, "opentsdb.input")

      val localFs = FileSystem.getLocal(new Configuration)

      localFs.mkdirs(dataDirPath)
      localFs.copyFromLocalFile(false, true, new Path("data/opentsdb.input"), dataFilePath)

      val rdd = sparkSession.sparkContext.textFile(s"file://${dataFilePath.toString}")

      val splittedLines = rdd.map {
        _.split(' ')
      }

      val values2 = splittedLines.
        filter(splittedLine => splittedLine(0) == "open" && splittedLine(3) == "symbol=AAPL").
        map(splittedLine => (splittedLine(1).toLong, splittedLine(2).toDouble)).map(_._2).collect()

      values1.length must be(values2.length)

      for (i <- values1.indices) {
        val diff = Math.abs(values1(i) - values2(i))
        println(s"$i ${values1(i)} ${values2(i)} $diff")
        diff must be < (0.00001d)
      }
    }
  }

  "Spark" must {
    "load a timeseries dataframe from OpenTSDB correctly" in {

      for (i <- 0 until 10) {
        val ts = Timestamp.from(Instant.parse(s"2016-07-05T${10 + i}:00:00.00Z"))
        val epoch = ts.getTime
        tsdb.addPoint("mymetric", epoch, i.toLong, Map("key1" -> "value1", "key2" -> "value2").asJava).joinUninterruptibly()
      }

      val tsStart = Timestamp.from(Instant.parse(s"2016-07-05T10:00:00.00Z"))
      val tsEnd = Timestamp.from(Instant.parse(s"2016-07-05T20:00:00.00Z"))

      val df = openTSDBContext.loadDataFrame("mymetric", Map("key1" -> "value1", "key2" -> "value2"), Some(tsStart --> tsEnd))

      df.schema must be(
        StructType(
          Array(
            StructField("timestamp", TimestampType, nullable = false),
            StructField("metric", StringType, nullable = false),
            StructField("value", DoubleType, nullable = false),
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
    "load a timeseries dataframe from OpenTSDB using DefaultSource correctly" in {

      openTSDBContext.createMetrics(List(("mymetric1", Map("key1" -> "value1", "key2" -> "value2"))))

      for (i <- 0 until 10) {
        val ts = Timestamp.from(Instant.parse(s"2016-07-05T${10 + i}:00:00.00Z"))
        val epoch = ts.getTime
        tsdb.addPoint("mymetric1", epoch, i.toLong, Map("key1" -> "value1", "key2" -> "value2").asJava).join()
      }

      val tsStart = Timestamp.from(Instant.parse(s"2016-07-05T09:00:00.00Z")).getTime / 1000
      val tsEnd = Timestamp.from(Instant.parse(s"2016-07-05T20:00:00.00Z")).getTime / 1000

      val df = sparkSession.read.options(Map(
        "opentsdb.metric" -> "mymetric1",
        "opentsdb.tags" -> "key1->value1,key2->value2",
        "opentsdb.interval" -> s"$tsStart:$tsEnd"
      )).opentsdb

      df.schema must be(
        StructType(
          Array(
            StructField("timestamp", TimestampType, nullable = false),
            StructField("metric", StringType, nullable = false),
            StructField("value", DoubleType, nullable = false),
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
    "save timeseries points using DefaultSource correctly" in {

      openTSDBContext.createMetrics(List(("mymetric2", Map("key1" -> "value1", "key2" -> "value2"))))

      val points = for {
        i <- 0 until 10
        ts = Timestamp.from(Instant.parse(s"2016-07-05T${10 + i}:00:00.00Z"))
        epoch = ts.getTime
        point = DataPoint("mymetric2", epoch, i.toDouble, Map("key1" -> "value1", "key2" -> "value2"))
      } yield point

      val rdd = sparkSession.sparkContext.parallelize[DataPoint[Double]](points)

      rdd.toDF.write.options(Map.empty[String, String]).mode("append").opentsdb()

      val tsStart = Timestamp.from(Instant.parse(s"2016-07-05T10:00:00.00Z"))
      val tsEnd = Timestamp.from(Instant.parse(s"2016-07-05T20:00:00.00Z"))

      val df = openTSDBContext.loadDataFrame("mymetric2", Map("key1" -> "value1", "key2" -> "value2"), Some(tsStart --> tsEnd))

      df.schema must be(
        StructType(
          Array(
            StructField("timestamp", TimestampType, nullable = false),
            StructField("metric", StringType, nullable = false),
            StructField("value", DoubleType, nullable = false),
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

      openTSDBContext.createMetrics(List(("mymetric3", Map("key1" -> "value1", "key2" -> "value2"))))

      val points = for {
        i <- 0 until 10
        ts = Timestamp.from(Instant.parse(s"2016-07-05T${10 + i}:00:00.00Z"))
        epoch = ts.getTime
        point = DataPoint("mymetric3", epoch, i.toDouble, Map("key1" -> "value1", "key2" -> "value2"))
      } yield point

      val rdd = sparkSession.sparkContext.parallelize[DataPoint[Double]](points)

      @SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
      val stream = streamingContext.queueStream[DataPoint[Double]](mutable.Queue(rdd))

      openTSDBContext.streamWrite(stream)

      streamingContext.start()

      Thread.sleep(1000)

      val tsStart = Timestamp.from(Instant.parse(s"2016-07-05T10:00:00.00Z"))
      val tsEnd = Timestamp.from(Instant.parse(s"2016-07-05T20:00:00.00Z"))

      val df = openTSDBContext.loadDataFrame("mymetric3", Map("key1" -> "value1", "key2" -> "value2"), Some(tsStart --> tsEnd))

      df.schema must be(
        StructType(
          Array(
            StructField("timestamp", TimestampType, nullable = false),
            StructField("metric", StringType, nullable = false),
            StructField("value", DoubleType, nullable = false),
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