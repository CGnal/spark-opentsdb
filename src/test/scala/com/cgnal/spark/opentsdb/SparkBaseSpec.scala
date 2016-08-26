/*
 * Copyright 2016 CGnal S.p.A.
 *
 */

package com.cgnal.spark.opentsdb

import net.opentsdb.core.TSDB
import net.opentsdb.utils.Config
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.{ HBaseTestingUtility, TableName }
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{ Milliseconds, StreamingContext }
import org.apache.spark.{ SparkConf, SparkContext }
import org.scalatest.{ BeforeAndAfterAll, MustMatchers, WordSpec }
import shaded.org.hbase.async.HBaseClient

@SuppressWarnings(Array("org.wartremover.warts.Var"))
trait SparkBaseSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {

  val hbaseUtil = new HBaseTestingUtility()

  var sparkContext: SparkContext = _

  var streamingContext: StreamingContext = _

  var hbaseContext: HBaseContext = _

  var openTSDBContext: OpenTSDBContext = _

  var sqlContext: SQLContext = _

  var hbaseAsyncClient: HBaseClient = _

  var tsdb: TSDB = _

  override def beforeAll(): Unit = {
    OpenTSDBContext.saltWidth = 1
    OpenTSDBContext.saltBuckets = 2
    hbaseUtil.startMiniCluster(1)
    val conf = new SparkConf().
      setAppName("spark-opentsdb-local-test").
      setMaster("local").
      set("spark.io.compression.codec", "lzf")
    sparkContext = new SparkContext(conf)
    streamingContext = new StreamingContext(sparkContext, Milliseconds(200))
    hbaseContext = new HBaseContext(sparkContext, hbaseUtil.getConfiguration)
    sqlContext = new SQLContext(sparkContext)
    openTSDBContext = new OpenTSDBContext(sqlContext, Some(hbaseUtil.getConfiguration))
    hbaseUtil.createTable(TableName.valueOf("tsdb-uid"), Array("id", "name"))
    hbaseUtil.createTable(TableName.valueOf("tsdb"), Array("t"))
    hbaseUtil.createTable(TableName.valueOf("tsdb-tree"), Array("t"))
    hbaseUtil.createTable(TableName.valueOf("tsdb-meta"), Array("name"))
    val quorum = hbaseUtil.getConfiguration.get("hbase.zookeeper.quorum")
    val port = hbaseUtil.getConfiguration.get("hbase.zookeeper.property.clientPort")
    hbaseAsyncClient = new HBaseClient(s"$quorum:$port", "/hbase")
    val config = new Config(false)
    config.overrideConfig("tsd.storage.hbase.data_table", "tsdb")
    config.overrideConfig("tsd.storage.hbase.uid_table", "tsdb-uid")
    config.overrideConfig("tsd.core.auto_create_metrics", "true")
    if (openTSDBContext.saltWidth > 0) {
      config.overrideConfig("tsd.storage.salt.width", openTSDBContext.saltWidth.toString)
      config.overrideConfig("tsd.storage.salt.buckets", openTSDBContext.saltBuckets.toString)
    }
    config.disableCompactions()
    tsdb = new TSDB(hbaseAsyncClient, config)
  }

  override def afterAll(): Unit = {
    streamingContext.stop(false)
    streamingContext.awaitTermination()
    sparkContext.stop()
    tsdb.shutdown()
    hbaseUtil.deleteTable("tsdb-uid")
    hbaseUtil.deleteTable("tsdb")
    hbaseUtil.deleteTable("tsdb-tree")
    hbaseUtil.deleteTable("tsdb-meta")
    hbaseUtil.shutdownMiniCluster()
  }

}