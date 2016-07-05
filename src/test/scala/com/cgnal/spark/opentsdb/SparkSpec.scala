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

import java.util.{Calendar, Date}

import net.opentsdb.core.TSDB
import net.opentsdb.utils.Config
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.{HBaseTestingUtility, TableName}
import org.apache.spark.{SparkConf, SparkContext}
import org.hbase.async.HBaseClient
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpec}

import scala.collection.JavaConversions._

class SparkSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {

  val hbaseUtil = new HBaseTestingUtility()

  var sparkContext: SparkContext = _

  var hbaseContext: HBaseContext = _

  var openTSDBContext: OpenTSDBContext = _

  var tsdb: TSDB = _

  override def beforeAll(): Unit = {
    hbaseUtil.startMiniCluster(10)
    val conf = new SparkConf().
      setAppName("spark-cdh5-template-local-test").
      setMaster("local")
    sparkContext = new SparkContext(conf)
    hbaseContext = new HBaseContext(sparkContext, hbaseUtil.getConfiguration)
    openTSDBContext = new OpenTSDBContext(hbaseContext)
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
        val ts = openTSDBContext.generateRDD("mymetric", "key1->value1, key2->value2", "05/07/2016 10:00", "05/07/2016 20:00")

        val result = ts.collect()

        result.length must be(10)

        result.foreach(p => println((simpleDateFormat.format(new Date(p._1 * 1000)), p._2)))
      }
      println("------------")

      {
        val simpleDateFormat = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm")
        val ts = openTSDBContext.generateRDD("mymetric", "key1->value1", "05/07/2016 10:00", "06/07/2016 20:00")

        val result = ts.collect()

        result.length must be(20)

        result.foreach(p => println((simpleDateFormat.format(new Date(p._1 * 1000)), p._2)))
      }
      println("------------")

      {
        val simpleDateFormat = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm")
        val ts = openTSDBContext.generateRDD("mymetric", "key1->value1, key3 -> value3", "05/07/2016 10:00", "06/07/2016 20:00")

        val result = ts.collect()

        result.length must be(10)

        result.foreach(p => println((simpleDateFormat.format(new Date(p._1 * 1000)), p._2)))
      }
      println("------------")

      {
        val simpleDateFormat = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm")
        val ts = openTSDBContext.generateRDD("mymetric", "key1->value1, key2 -> value2", "05/07/2016 10:00", "06/07/2016 20:00")

        val result = ts.collect()

        result.length must be(10)

        result.foreach(p => println((simpleDateFormat.format(new Date(p._1 * 1000)), p._2)))
      }

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