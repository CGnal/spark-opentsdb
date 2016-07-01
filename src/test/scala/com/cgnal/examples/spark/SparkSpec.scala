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

package com.cgnal.examples.spark

import java.time.{ LocalDateTime, ZoneOffset }

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{ HBaseTestingUtility, TableName }
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import org.scalatest.{ BeforeAndAfterAll, MustMatchers, WordSpec }

import scala.collection.immutable.IndexedSeq

class SparkSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {

  val hbaseUtil = new HBaseTestingUtility()

  var sparkContext: SparkContext = _

  var hbaseContext: HBaseContext = _

  override def beforeAll(): Unit = {
    hbaseUtil.startMiniCluster(10)
    val conf = new SparkConf().
      setAppName("spark-cdh5-template-local-test").
      setMaster("local")
    sparkContext = new SparkContext(conf)
    hbaseContext = new HBaseContext(sparkContext, hbaseUtil.getConfiguration)
    ()
  }

  "Spark" must {
    "load from an HBase table correctly" in {
      hbaseUtil.createTable(TableName.valueOf("MyTable"), Array("MYCF"))
      val table = hbaseUtil.getConnection.getTable(TableName.valueOf("MyTable"))
      for (i <- 1 to 10) {
        val p = new Put(Bytes.toBytes(i))
        p.addColumn(Bytes.toBytes("MYCF"), Bytes.toBytes("QF1"), Bytes.toBytes(s"CIAO$i"))
        table.put(p)
      }
      val rdd = hbaseContext.hbaseRDD(TableName.valueOf("MyTable"), new Scan()).asInstanceOf[RDD[(ImmutableBytesWritable, Result)]]
      rdd.map(p => Bytes.toInt(p._2.getRow)).collect().toList must be(1 to 10)
      hbaseUtil.deleteTable(TableName.valueOf("MyTable"))
    }
  }

  val NUMBUCKETS = 10

  "Spark" must {
    "support key salting correctly" in {
      hbaseUtil.createTable(TableName.valueOf("MyTable"), Array("MYCF"))
      val table: Table = hbaseUtil.getConnection.getTable(TableName.valueOf("MyTable"))
      val n = 100
      val now = LocalDateTime.now()
      for (i <- 1 to n) {
        val rowKey = computeKey(s"ID$i", now.minusDays(i.toLong - 1))
        val p = new Put(rowKey)
        p.addColumn(Bytes.toBytes("MYCF"), Bytes.toBytes("QF1"), Bytes.toBytes(i))
        table.put(p)
      }

      val startDateTime = now.minusDays(n.toLong)
      val stopDateTime = now.plusSeconds(1)

      val scans = getScans(startDateTime, stopDateTime)

      val initRdd = hbaseContext.hbaseRDD(TableName.valueOf("MyTable"), scans.head).asInstanceOf[RDD[(ImmutableBytesWritable, Result)]]
      val rdds = scans.tail.map(scan => hbaseContext.hbaseRDD(TableName.valueOf("MyTable"), scan).asInstanceOf[RDD[(ImmutableBytesWritable, Result)]])
      val unionRdd = rdds.fold(initRdd)((rdd1, rdd2) => rdd1.union(rdd2))

      val values = unionRdd.map(p => {
        val value = p._2.getValue(Bytes.toBytes("MYCF"), Bytes.toBytes("QF1"))
        Bytes.toInt(value)
      }).collect().sorted

      values must be(1 to n)

      hbaseUtil.deleteTable(TableName.valueOf("MyTable"))
    }
  }

  private def computeKey(documentId: String, timestamp: LocalDateTime): Array[Byte] = {
    val bucket: Int = documentId.hashCode % NUMBUCKETS
    val epoch: Long = timestamp.toEpochSecond(ZoneOffset.UTC)
    val id: Array[Byte] = Bytes.toBytes(documentId)
    val buffer = new Array[Byte](4 + 8 + id.length)
    Bytes.putInt(buffer, 0, bucket)
    Bytes.putLong(buffer, 4, epoch)
    Bytes.putBytes(buffer, 12, id, 0, id.length)
    buffer
  }

  private def getScans(startLocalDate: LocalDateTime, endLocalDate: LocalDateTime): IndexedSeq[Scan] = {
    (0 until NUMBUCKETS).map(bucket => {
      val startRowKey = Bytes.toBytes(bucket) ++ Bytes.toBytes(startLocalDate.toEpochSecond(ZoneOffset.UTC))
      val endRowKey = Bytes.toBytes(bucket) ++ Bytes.toBytes(endLocalDate.toEpochSecond(ZoneOffset.UTC))
      new Scan(startRowKey, endRowKey)
    })
  }

  override def afterAll(): Unit = {
    sparkContext.stop()
    hbaseUtil.shutdownMiniCluster()
  }

}

object SparkSpec {
  var connection: Option[Connection] = None
}
