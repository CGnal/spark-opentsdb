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
import java.time.{ ZoneId, ZonedDateTime }
import java.util.{ Calendar, Date }

import net.opentsdb.core.TSDB
import net.opentsdb.utils.Config
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.{ HBaseTestingUtility, TableName }
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.{ SparkConf, SparkContext }
import org.hbase.async.HBaseClient
import org.scalatest.{ BeforeAndAfterAll, MustMatchers, WordSpec }

import scala.collection.JavaConversions._

class SparkSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {

  val hbaseUtil = new HBaseTestingUtility()

  var sparkContext: SparkContext = _

  var hbaseContext: HBaseContext = _

  var openTSDBContext: OpenTSDBContext = _

  var sqlContext: SQLContext = _

  var tsdb: TSDB = _

  override def beforeAll(): Unit = {
    hbaseUtil.startMiniCluster(10)
    val conf = new SparkConf().
      setAppName("spark-cdh5-template-local-test").
      setMaster("local")
    sparkContext = new SparkContext(conf)
    hbaseContext = new HBaseContext(sparkContext, hbaseUtil.getConfiguration)
    openTSDBContext = new OpenTSDBContext(hbaseContext)
    sqlContext = new SQLContext(sparkContext)
    hbaseUtil.createTable(TableName.valueOf("tsdb-uid"), Array("id", "name"))
    hbaseUtil.createTable(TableName.valueOf("tsdb"), Array("t"))
    hbaseUtil.createTable(TableName.valueOf("tsdb-tree"), Array("t"))
    hbaseUtil.createTable(TableName.valueOf("tsdb-meta"), Array("name"))
    val quorum = hbaseUtil.getConfiguration.get("hbase.zookeeper.quorum")
    val port = hbaseUtil.getConfiguration.get("hbase.zookeeper.property.clientPort")
    val hbaseAsyncClient = new HBaseClient(s"$quorum:$port", "/hbase")
    val config = new Config(false)
    config.overrideConfig("tsd.storage.hbase.data_table", "tsdb")
    config.overrideConfig("tsd.storage.hbase.uid_table", "tsdb-uid")
    config.overrideConfig("tsd.core.auto_create_metrics", "true")
    tsdb = new TSDB(hbaseAsyncClient, config)
  }

  "Spark" must {
    "load a timeseries from OpenTSDB correctly" in {

      for (i <- 0 until 10) {
        val epoch: Long = new Calendar.Builder().setDate(2016, 6, 5).setTimeOfDay(10 + i, 0, 0).build().getTime.getTime / 1000
        tsdb.addPoint("mymetric", epoch, i.toLong, Map("key1" -> "value1", "key2" -> "value2"))
      }

      for (i <- 0 until 10) {
        val epoch: Long = new Calendar.Builder().setDate(2016, 6, 6).setTimeOfDay(10 + i, 0, 0).build().getTime.getTime / 1000
        tsdb.addPoint("mymetric", epoch, (i + 100).toLong, Map("key1" -> "value1", "key3" -> "value3"))
      }

      // Default Date Format: dd/MM/yyyy HH:mm
      {
        val simpleDateFormat = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm")
        val ts = openTSDBContext.load("mymetric", Map("key1" -> "value1", "key2" -> "value2"), Some("05/07/2016 10:00"), Some("05/07/2016 20:00"))

        val result = ts.collect()

        result.length must be(10)

        result.foreach(p => println((simpleDateFormat.format(new Date(p._1 * 1000)), p._2)))
      }
      println("------------")

      {
        val simpleDateFormat = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm")
        val ts = openTSDBContext.load("mymetric", Map("key1" -> "value1"), Some("05/07/2016 10:00"), Some("06/07/2016 20:00"))

        val result = ts.collect()

        result.length must be(20)

        result.foreach(p => println((simpleDateFormat.format(new Date(p._1 * 1000)), p._2)))
      }
      println("------------")

      {
        val simpleDateFormat = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm")
        val ts = openTSDBContext.load("mymetric", Map("key1" -> "value1", "key3" -> "value3"), Some("05/07/2016 10:00"), Some("06/07/2016 20:00"))

        val result = ts.collect()

        result.length must be(10)

        result.foreach(p => println((simpleDateFormat.format(new Date(p._1 * 1000)), p._2)))
      }
      println("------------")

      {
        val simpleDateFormat = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm")
        val ts = openTSDBContext.load("mymetric", Map("key1" -> "value1", "key2" -> "value2"), Some("05/07/2016 10:00"), Some("06/07/2016 20:00"))

        val result = ts.collect()

        result.length must be(10)

        result.foreach(p => println((simpleDateFormat.format(new Date(p._1 * 1000)), p._2)))
      }
      println("------------")

      {
        val simpleDateFormat = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm")
        val ts = openTSDBContext.load("mymetric")

        val result = ts.collect()

        result.length must be(20)

        result.foreach(p => println((simpleDateFormat.format(new Date(p._1 * 1000)), p._2)))
      }

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
          val price = tokens(5).toFloat
          Row(Timestamp.from(dt.toInstant), symbol, price)
        }
        val fields = Seq(
          StructField("timestamp", TimestampType, true),
          StructField("symbol", StringType, true),
          StructField("price", FloatType, true)
        )
        val schema = StructType(fields)
        sqlContext.createDataFrame(rowRdd, schema)
      }

      val tickerObs = loadObservations(sqlContext, "data/ticker.tsv")

      //"timestamp", "symbol", "price"
      //(String, Date, Float, Map[String, String])
      val timeseries = tickerObs.map(row => ("ticker", row.getAs[Timestamp]("timestamp").getTime / 1000, row.getAs[Float]("price"), Map("symbol" -> row.getAs[String]("symbol"))))

      openTSDBContext.write(timeseries)

      tickerObs.registerTempTable("tickerObs")

      val ts1 = sqlContext.sql("select * from tickerObs where symbol = 'CSCO' sort by(timestamp)").collect()

      val ts2 = openTSDBContext.load(metricName = "ticker", tags = Map("symbol" -> "CSCO")).collect()

      ts1.foreach(println(_))

      println("---------")

      val simpleDateFormat = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm")
      ts2.foreach(p => println((simpleDateFormat.format(new Date(p._1 * 1000)), p._2)))

      ts1.length must be(ts2.length)
    }
  }

  override def afterAll(): Unit = {
    sparkContext.stop()
    hbaseUtil.deleteTable("tsdb-uid")
    hbaseUtil.deleteTable("tsdb")
    hbaseUtil.deleteTable("tsdb-tree")
    hbaseUtil.deleteTable("tsdb-meta")
    hbaseUtil.shutdownMiniCluster()
  }

}