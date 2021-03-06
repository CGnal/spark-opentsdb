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

import net.opentsdb.core.TSDB
import net.opentsdb.utils.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{ HBaseConfiguration, HBaseTestingUtility, TableName }
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ Milliseconds, StreamingContext }
import org.scalatest.{ BeforeAndAfterAll, MustMatchers, WordSpec }
import shaded.org.hbase.async.HBaseClient

import scala.collection.convert.decorateAsScala._

@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.NonUnitStatements", "org.wartremover.warts.Null"))
trait SparkBaseSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {

  private val hbaseUtil = new HBaseTestingUtility()

  var streamingContext: StreamingContext = _

  var baseConf: Configuration = _

  var openTSDBContext: OpenTSDBContext = _

  implicit var sparkSession: SparkSession = _

  var hbaseAsyncClient: HBaseClient = _

  var tsdb: TSDB = _

  override def beforeAll(): Unit = {
    OpenTSDBContext.saltWidth = 1
    OpenTSDBContext.saltBuckets = 2
    OpenTSDBContext.preloadUidCache = true
    hbaseUtil.startMiniCluster(4)
    val conf = new SparkConf().
      setAppName("spark-opentsdb-local-test").
      setMaster("local[4]").
      set("spark.io.compression.codec", "lzf")

    baseConf = hbaseUtil.getConfiguration

    val quorum = baseConf.get("hbase.zookeeper.quorum")
    val port = baseConf.get("hbase.zookeeper.property.clientPort")

    sparkSession = SparkSession.builder().config(conf).getOrCreate()

    HBaseConfiguration.merge(sparkSession.sparkContext.hadoopConfiguration, baseConf)

    streamingContext = new StreamingContext(sparkSession.sparkContext, Milliseconds(200))
    openTSDBContext = new OpenTSDBContext(sparkSession, TestOpenTSDBConfigurator(baseConf))
    hbaseUtil.createTable(TableName.valueOf("tsdb-uid"), Array("id", "name"))
    hbaseUtil.createTable(TableName.valueOf("tsdb"), Array("t"))
    hbaseUtil.createTable(TableName.valueOf("tsdb-tree"), Array("t"))
    hbaseUtil.createTable(TableName.valueOf("tsdb-meta"), Array("name"))
    hbaseAsyncClient = new HBaseClient(s"$quorum:$port", "/hbase")
    val config = new Config(false)
    config.overrideConfig("tsd.storage.hbase.data_table", "tsdb")
    config.overrideConfig("tsd.storage.hbase.uid_table", "tsdb-uid")
    config.overrideConfig("tsd.core.auto_create_metrics", "true")
    if (openTSDBContext.saltWidth > 0) {
      config.overrideConfig("tsd.storage.salt.width", openTSDBContext.saltWidth.toString)
      config.overrideConfig("tsd.storage.salt.buckets", openTSDBContext.saltBuckets.toString)
    }
    config.overrideConfig("batchSize", "10")
    config.disableCompactions()
    tsdb = new TSDB(hbaseAsyncClient, config)
  }

  override def afterAll(): Unit = {
    streamingContext.stop(false)
    streamingContext.awaitTermination()
    sparkSession.stop()
    tsdb.shutdown()
    hbaseUtil.deleteTable("tsdb-uid")
    hbaseUtil.deleteTable("tsdb")
    hbaseUtil.deleteTable("tsdb-tree")
    hbaseUtil.deleteTable("tsdb-meta")
    hbaseUtil.shutdownMiniCluster()
  }

}

class TestOpenTSDBConfigurator(mapConf: Map[String, String]) extends OpenTSDBConfigurator with Serializable {

  lazy val configuration: Configuration = mapConf.foldLeft(new Configuration(false)) { (conf, pair) =>
    conf.set(pair._1, pair._2)
    conf
  }

}

object TestOpenTSDBConfigurator {

  def apply(conf: Configuration): TestOpenTSDBConfigurator = new TestOpenTSDBConfigurator(
    conf.iterator().asScala.map { entry => entry.getKey -> entry.getValue }.toMap[String, String]
  )

}