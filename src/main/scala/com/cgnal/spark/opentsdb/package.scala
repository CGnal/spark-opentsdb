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

import net.opentsdb.core.TSDB
import net.opentsdb.utils.Config
import org.hbase.async.HBaseClient
import shapeless.{ :+:, CNil }

package object opentsdb {

  type Value = Byte :+: Int :+: Long :+: Float :+: Double :+: CNil

  implicit val writeForByte: (Iterator[(String, Long, Byte, Map[String, String])], TSDB) => Unit = (it, tsdb) => {
    import collection.JavaConversions._
    it.foreach(record => {
      tsdb.addPoint(record._1, record._2, record._3.asInstanceOf[Long], record._4)
    })
  }

  implicit val writeForInt: (Iterator[(String, Long, Int, Map[String, String])], TSDB) => Unit = (it, tsdb) => {
    import collection.JavaConversions._
    it.foreach(record => {
      tsdb.addPoint(record._1, record._2, record._3.asInstanceOf[Long], record._4)
    })
  }

  implicit val writeForLong: (Iterator[(String, Long, Long, Map[String, String])], TSDB) => Unit = (it, tsdb) => {
    import collection.JavaConversions._
    it.foreach(record => {
      tsdb.addPoint(record._1, record._2, record._3, record._4)
    })
  }

  implicit val writeForFloat: (Iterator[(String, Long, Float, Map[String, String])], TSDB) => Unit = (it, tsdb) => {
    import collection.JavaConversions._
    it.foreach(record => {
      tsdb.addPoint(record._1, record._2, record._3, record._4)
    })
  }

  implicit val writeForDouble: (Iterator[(String, Long, Double, Map[String, String])], TSDB) => Unit = (it, tsdb) => {
    import collection.JavaConversions._
    it.foreach(record => {
      tsdb.addPoint(record._1, record._2, record._3, record._4)
    })
  }

  var tsdbTable = "tsdb"

  var tsdbUidTable = "tsdb-uid"

  private[opentsdb] object TSDBClientManager {

    var quorum: String = _

    var port: String = _

    lazy val tsdb: TSDB = {
      val hbaseAsyncClient = new HBaseClient(s"$quorum:$port", "/hbase")
      val config = new Config(false)
      config.overrideConfig("tsd.storage.hbase.data_table", tsdbTable)
      config.overrideConfig("tsd.storage.hbase.uid_table", tsdbUidTable)
      config.overrideConfig("tsd.core.auto_create_metrics", "true")
      new TSDB(hbaseAsyncClient, config)
    }

    def apply(quorum: String, port: String): Unit = {
      this.quorum = quorum
      this.port = port
    }

  }

}
