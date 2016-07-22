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
import java.time.{ Instant, ZoneId, ZonedDateTime }
import java.util.TimeZone

import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }

import scala.collection.JavaConversions._

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

        result.foreach(p => println((simpleDateFormat.format(p.getAs[Timestamp](0)), p.getAs[Long](1))))
      }
      println("------------")

      {
        val simpleDateFormat = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm")
        val ts = openTSDBContext.load("mymetric", Map("key1" -> "value1"), Some("05/07/2016 10:00"), Some("06/07/2016 20:00"))

        val result = ts.collect()

        result.length must be(20)

        result.foreach(p => println((simpleDateFormat.format(p.getAs[Timestamp](0)), p.getAs[Long](1))))
      }
      println("------------")

      {
        val simpleDateFormat = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm")
        val ts = openTSDBContext.load("mymetric", Map("key1" -> "value1", "key3" -> "value3"), Some("05/07/2016 10:00"), Some("06/07/2016 20:00"))

        val result = ts.collect()

        result.length must be(10)

        result.foreach(p => println((simpleDateFormat.format(p.getAs[Timestamp](0)), p.getAs[Long](1))))
      }
      println("------------")

      {
        val simpleDateFormat = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm")
        val ts = openTSDBContext.load("mymetric", Map("key1" -> "value1", "key2" -> "value2"), Some("05/07/2016 10:00"), Some("06/07/2016 20:00"))

        val result = ts.collect()

        result.length must be(10)

        result.foreach(p => println((simpleDateFormat.format(p.getAs[Timestamp](0)), p.getAs[Long](1))))
      }
      println("------------")

      {
        val simpleDateFormat = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm")
        val ts = openTSDBContext.load("mymetric")

        val result = ts.collect()

        result.length must be(20)

        result.foreach(p => println((simpleDateFormat.format(p.getAs[Timestamp](0)), p.getAs[Long](1))))
      }
    }
  }

  "Spark" must {
    "load a timeseries with milliseconds granularity correctly" in {
      for (i <- 0 until 10)
        tsdb.addPoint("anothermetric", i.toLong, (i - 10).toFloat, Map("key1" -> "value1", "key2" -> "value2")).joinUninterruptibly()

      val ts = openTSDBContext.load("anothermetric", Map("key1" -> "value1", "key2" -> "value2"), None, None, conversionStrategy = ConvertToDouble)

      val result = ts.collect()

      result.map(r => (r.getAs[Timestamp](0).getTime, r.getAs[Double](1))) must be((0 until 10).map(i => (i.toLong, (i - 10).toDouble)))
    }
  }

  "Spark" must {
    "load a timeseries from OpenTSDB into a Spark Timeseries RDD correctly" in {

      /**
       * Creates a Spark DataFrame of (timestamp, symbol, price) from a tab-separated file of stock
       * ticker data.
       */
      def loadObservations(sqlContext: SQLContext, path: String): DataFrame = {
        val rowRdd = sqlContext.sparkContext.textFile(path).map { line =>
          val tokens = line.split('\t')
          val dt = ZonedDateTime.of(tokens(0).toInt, tokens(1).toInt, tokens(2).toInt, 0, 0, 0, 0,
            ZoneId.systemDefault())
          val symbol = tokens(3)
          val price = tokens(5).toDouble
          Row(Timestamp.from(dt.toInstant), symbol, price)
        }
        val fields = Seq(
          StructField("timestamp", TimestampType, nullable = true),
          StructField("symbol", StringType, nullable = true),
          StructField("price", DoubleType, nullable = true)
        )
        val schema = StructType(fields)
        sqlContext.createDataFrame(rowRdd, schema)
      }

      val tickerObs = loadObservations(sqlContext, "data/ticker.tsv")

      //"timestamp", "symbol", "price"
      //(String, Date, Double, Map[String, String])
      val timeseries = tickerObs.map(row => ("ticker", row.getAs[Timestamp]("timestamp").getTime / 1000, row.getAs[Double]("price"), Map("symbol" -> row.getAs[String]("symbol"))))

      openTSDBContext.write(timeseries)

      tickerObs.registerTempTable("tickerObs")

      val ts1 = sqlContext.sql("select * from tickerObs where symbol = 'CSCO' sort by(timestamp)").collect()

      val ts2 = openTSDBContext.load(metricName = "ticker", tags = Map("symbol" -> "CSCO")).collect()

      ts1.foreach(println(_))

      println("---------")

      val simpleDateFormat = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm")
      ts2.foreach(p => println((simpleDateFormat.format(p.getAs[Timestamp](0)), p.getAs[Double](1))))

      ts1.length must be(ts2.length)
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
        val df = openTSDBContext.loadDataFrame("mymetric", Map("key1" -> "value1", "key2" -> "value2"), Some("05/07/2016 10:00"), Some("05/07/2016 20:00"), conversionStrategy = ConvertToFloat)

        df.schema must be(StructType(Array(StructField("timestamp", TimestampType, false), StructField("value", FloatType, false))))

        val result = df.collect()

        result.length must be(10)

        result.foreach(println(_))

      }
    }

  }

}