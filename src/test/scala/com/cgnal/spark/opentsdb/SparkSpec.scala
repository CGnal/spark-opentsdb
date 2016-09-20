/*
 * Copyright 2016 CGnal S.p.A.
 *
 */

package com.cgnal.spark.opentsdb

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.Instant
import java.util.TimeZone

import com.cloudera.sparkts.MillisecondFrequency
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

      df.registerTempTable("open")

      val out = sqlContext.sql("select tags['symbol'], timestamp, value from open where tags['symbol'] = 'AAPL'")

      val values1 = out.map(_.getAs[Double](2)).take(5625) //.collect() //TODO this test fails with collect the, is it a consequence of running inside a test?

      val mapredWorkingDir = new Path(sparkContext.hadoopConfiguration.get("mapred.working.dir"))

      val dataDirPath = new Path(mapredWorkingDir, "data")
      val dataFilePath = new Path(dataDirPath, "opentsdb.input")

      val localFs = FileSystem.getLocal(new Configuration)

      localFs.mkdirs(dataDirPath)
      localFs.copyFromLocalFile(false, true, new Path("data/opentsdb.input"), dataFilePath)

      val rdd = sparkContext.textFile(s"file://${dataFilePath.toString}")

      val splittedLines = rdd.map { _.split(' ') }

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
    "load timeseries from OpenTSDB into a Spark Timeseries RDD correctly" in {

      for (i <- 0 until 1000) {
        tsdb.addPoint("metric1", i.toLong, (i - 10).toFloat, Map("key1" -> "value1").asJava)
        tsdb.addPoint("metric2", i.toLong, (i - 20).toFloat, Map("key1" -> "value1").asJava)
        tsdb.addPoint("metric3", i.toLong, (i - 30).toFloat, Map("key1" -> "value1").asJava)
        tsdb.addPoint("metric4", i.toLong, (i - 40).toFloat, Map("key1" -> "value1").asJava)
        tsdb.addPoint("metric5", i.toLong, (i - 50).toFloat, Map("key1" -> "value1").asJava)
      }

      val startDate = Timestamp.from(Instant.parse(s"1970-01-01T00:00:00.000Z"))
      val endDate = Timestamp.from(Instant.parse(s"1970-01-01T00:00:01.00Z"))
      val simpleDateFormat = new SimpleDateFormat("yyyy-MM-ss mm-ss-SSS")
      simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))

      val ts = openTSDBContext.loadTimeSeriesRDD(
        Some(startDate --> endDate),
        new MillisecondFrequency(1),
        List(
          "metric1" -> Map("key1" -> "value1"),
          "metric2" -> Map("key1" -> "value1"),
          "metric3" -> Map("key1" -> "value1"),
          "metric4" -> Map("key1" -> "value1"),
          "metric5" -> Map("key1" -> "value1")
        )
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

      //      DefaultSource.configuration = Some(hbaseUtil.getConfiguration)

      for (i <- 0 until 10) {
        val ts = Timestamp.from(Instant.parse(s"2016-07-05T${10 + i}:00:00.00Z"))
        val epoch = ts.getTime
        tsdb.addPoint("mymetric1", epoch, i.toLong, Map("key1" -> "value1", "key2" -> "value2").asJava).join()
      }

      val tsStart = Timestamp.from(Instant.parse(s"2016-07-05T09:00:00.00Z")).getTime / 1000
      val tsEnd = Timestamp.from(Instant.parse(s"2016-07-05T20:00:00.00Z")).getTime / 1000

      val df = sqlContext.read.options(Map(
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

      val points = for {
        i <- 0 until 10
        ts = Timestamp.from(Instant.parse(s"2016-07-05T${10 + i}:00:00.00Z"))
        epoch = ts.getTime
        point = DataPoint("mymetric2", epoch, i.toDouble, Map("key1" -> "value1", "key2" -> "value2"))
      } yield point

      val rdd = sparkContext.parallelize[DataPoint[Double]](points)

      rdd.toDF(sqlContext).write.options(Map.empty[String, String]).mode("append").opentsdb()

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

      val points = for {
        i <- 0 until 10
        ts = Timestamp.from(Instant.parse(s"2016-07-05T${10 + i}:00:00.00Z"))
        epoch = ts.getTime
        point = DataPoint("mymetric3", epoch, i.toDouble, Map("key1" -> "value1", "key2" -> "value2"))
      } yield point

      val rdd = sparkContext.parallelize[DataPoint[Double]](points)

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