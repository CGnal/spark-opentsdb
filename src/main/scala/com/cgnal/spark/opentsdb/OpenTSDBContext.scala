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

import java.nio.ByteBuffer
import java.util
import java.util.Calendar

import com.cgnal.spark.opentsdb.OpenTSDBContext._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ Result, Scan }
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{ RegexStringComparator, RowFilter }
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

class OpenTSDBContext(hbaseContext: HBaseContext, dateFormat: String = "dd/MM/yyyy HH:mm") {

  def generateRDD(metricName: String, tagKeyValueMap: String, startdate: String, enddate: String, dateFormat: String = "ddMMyyyyHH:mm"): RDD[(Long, Double)] = {

    val tags: Map[String, String] = if (tagKeyValueMap.trim != "*")
      tagKeyValueMap.split(",").map(_.split("->")).map(l => (l(0).trim, l(1).trim)).toMap
    else
      Map("dummyKey" -> "dummyValue")

    val uidScan = getUIDScan(metricName, tags)

    val tsdbUID = hbaseContext.hbaseRDD(TableName.valueOf("tsdb-uid"), uidScan).asInstanceOf[RDD[(ImmutableBytesWritable, Result)]]

    val metricsUID = tsdbUID.map(l => l._2.getValue("id".getBytes(), "metrics".getBytes())).filter(_ != null).collect

    val tagKUIDs = tsdbUID.map(l => (new String(l._1.copyBytes()), l._2.getValue("id".getBytes(), "tagk".getBytes()))).filter(_._2 != null).collect.toMap

    val tagVUIDs = tsdbUID.map(l => (new String(l._1.copyBytes()), l._2.getValue("id".getBytes(), "tagv".getBytes()))).filter(_._2 != null).collect.toMap

    if (metricsUID.length == 0)
      throw new Exception(s"Metric not found: $metricName")

    if(!(tagKUIDs.size == tags.size && tagVUIDs.size == tags.size))
      throw new Exception("Some of the tags are not found")

    val metricScan = getMetricScan(
      tags,
      metricsUID,
      tagKUIDs,
      tagVUIDs,
      if (startdate.trim == "*")
        None
      else
        Option(startdate),
      if (enddate.trim == "*")
        None
      else
        Option(enddate)
    )

    val tsdb = hbaseContext.hbaseRDD(TableName.valueOf("tsdb"), metricScan).asInstanceOf[RDD[(ImmutableBytesWritable, Result)]]

    val ts: RDD[(Long, Double)] = tsdb.
      map(kv => (
        util.Arrays.copyOfRange(kv._1.copyBytes(), 3, 7),
        kv._2.getFamilyMap("t".getBytes())
      )).
      map({
        kv =>
          val row = new ArrayBuffer[(Long, Double)]
          val basetime: Long = ByteBuffer.wrap(kv._1).getInt.toLong
          val iterator = kv._2.entrySet().iterator()
          var ctr = 0
          while (iterator.hasNext) {
            ctr += 1
            val next = iterator.next()
            val a = next.getKey
            val b = next.getValue
            val _delta = processQuantifier(a)
            val delta = _delta.map(_._1)
            val value = processValues(_delta, b)

            for (i <- delta.indices)
              row += ((basetime + delta(i), value(i)))
          }
          row
      }).flatMap(_.map(kv => (kv._1, kv._2)))
    ts
  }

  private def getUIDScan(metricName: String, tags: Map[String, String]) = {
    val scan = new Scan()
    val name: String = String.format("^%s$", Array(metricName, tags.keys.mkString("|"), tags.values.mkString("|")).mkString("|"))
    val keyRegEx: RegexStringComparator = new RegexStringComparator(name)
    val rowFilter: RowFilter = new RowFilter(CompareOp.EQUAL, keyRegEx)
    scan.setFilter(rowFilter)
    scan
  }

  private def getMetricScan(
    tags: Map[String, String],
    metricsUID: Array[Array[Byte]],
    tagKUIDs: Map[String, Array[Byte]],
    tagVUIDs: Map[String, Array[Byte]],
    startdate: Option[String] = None,
    enddate: Option[String] = None
  ) = {
    val tagKKeys = tagKUIDs.keys.toArray
    val tagVKeys = tagVUIDs.keys.toArray
    val ntags = tags.filter(kv => tagKKeys.contains(kv._1) && tagVKeys.contains(kv._2))
    val tagKV = tagKUIDs.
      filter(kv => ntags.contains(kv._1)).
      map(k => (k._2, tagVUIDs(tags(k._1)))).
      map(l => l._1 ++ l._2).toList.sorted(Ordering.by((_: Array[Byte]).toIterable))
    val scan = new Scan()
    val name = if (tagKV.size > 0)
      String.format("^%s.*%s.*$", bytes2hex(metricsUID.last, "\\x"), bytes2hex(tagKV.flatten.toArray, "\\x"))
    else
      String.format("^%s.*$", bytes2hex(metricsUID.last, "\\x"))

    val keyRegEx: RegexStringComparator = new RegexStringComparator(name)
    val rowFilter: RowFilter = new RowFilter(CompareOp.EQUAL, keyRegEx)
    scan.setFilter(rowFilter)

    val simpleDateFormat = new java.text.SimpleDateFormat(dateFormat)

    val minDate = new Calendar.Builder().setDate(1970, 0, 1).setTimeOfDay(1, 0, 0).build().getTime
    val maxDate = new Calendar.Builder().setDate(2099, 11, 31).setTimeOfDay(23, 59, 0).build().getTime

    val stDateBuffer = ByteBuffer.allocate(4)
    stDateBuffer.putInt((simpleDateFormat.parse(if (startdate.isDefined) startdate.get else simpleDateFormat.format(minDate)).getTime / 1000).toInt)

    val endDateBuffer = ByteBuffer.allocate(4)
    endDateBuffer.putInt((simpleDateFormat.parse(if (enddate.isDefined) enddate.get else simpleDateFormat.format(maxDate)).getTime / 1000).toInt)

    scan.setStartRow(hexStringToByteArray(bytes2hex(metricsUID.last, "\\x") + bytes2hex(stDateBuffer.array(), "\\x") + bytes2hex(tagKV.flatten.toArray, "\\x")))
    scan.setStopRow(hexStringToByteArray(bytes2hex(metricsUID.last, "\\x") + bytes2hex(endDateBuffer.array(), "\\x") + bytes2hex(tagKV.flatten.toArray, "\\x")))
    scan
  }

}

object OpenTSDBContext {
  //TODO: changes operations on binary strings to bits
  private def processQuantifier(quantifier: Array[Byte]): Array[(Int, Boolean, Int)] = {
    //converting Byte Arrays to a Array of binary string
    val q = quantifier.map({ v => Integer.toBinaryString(v & 255 | 256).substring(1) })
    var i = 0
    val out = new ArrayBuffer[(Int, Boolean, Int)]
    while (i != q.length) {
      var value = -1
      var isInteger = true
      var valueLength = -1
      var isQuantifierSizeTypeSmall = true
      //If the 1st 4 bytes are in format "1111", the size of the column quantifier is 4 bytes. Else 2 bytes
      if (q(i).substring(0, 3).compareTo("1111") == 0) {
        isQuantifierSizeTypeSmall = false
      }

      if (isQuantifierSizeTypeSmall) {
        val v = q(i) + q(i + 1).substring(0, 4) //The 1st 12 bits represent the delta
        value = Integer.parseInt(v, 2) //convert the delta to Int (seconds)
        isInteger = q(i + 1).substring(4, 5) == "0" //The 13th bit represent the format of the value for the delta. 0=Integer, 1=Float
        valueLength = Integer.parseInt(q(i + 1).substring(5, 8), 2) //The last 3 bits represents the length of the value
        i = i + 2
      } else {
        val v = q(i).substring(4, 8) + q(i + 1) + q(i + 2) + q(i + 3).substring(0, 2) //The first 4 bits represents the size, the next 22 bits hold the delta
        value = Integer.parseInt(v, 2) / 1000 //convert the delta to Int (milliseconds -> seconds)
        isInteger = q(i + 3).substring(4, 5) == "0" //The 29th bit represent the format of the value for the delta. 0=Integer, 1=Float
        valueLength = Integer.parseInt(q(i + 3).substring(5, 8), 2) //The last 3 bits represents the length of the value
        i = i + 4
      }

      out += ((value, isInteger, valueLength + 1))
    }
    out.toArray
  }

  //TODO: changes operations on binary strings to bits
  private def processValues(quantifier: Array[(Int, Boolean, Int)], values: Array[Byte]): Array[Double] = {
    //converting Byte Arrays to a Array of binary string
    val v = values.map({ v => Integer.toBinaryString(v & 255 | 256).substring(1) }).mkString

    val out = new ArrayBuffer[Double]
    var i = 0
    var j = 0
    while (j < quantifier.length) {
      //Is the value represented as integer or float
      val isInteger = quantifier(j)._2
      //The number of Byte in which the value has been encoded
      val valueSize = quantifier(j)._3 * 8
      //Get the value for the current delta
      val _value = v.substring(i, i + valueSize).mkString
      val value = if (!isInteger) {
        //See https://blog.penjee.com/binary-numbers-floating-point-conversion/
        val sign = if (_value.substring(0, 1).compare("0") == 0) 1 else -1
        val exp = Integer.parseInt(_value.substring(1, 9), 2) - 127
        val significand = 1 + _value.substring(9).map({
          var i = 0
          m => {
            i += 1
            m.asDigit / math.pow(2.toDouble, i.toDouble)
          }
        }).sum
        sign * math.pow(2.toDouble, exp.toDouble) * significand
      } else {
        val o = Integer.parseInt(_value, 2).toDouble
        //#hotfix: signed interger ov value -1 is represented as 11111111, which gets converted to 255
        if (o == 255.0) -1.toDouble else o
      }

      i += valueSize
      j += 1

      out += value
    }
    out.toArray
  }

  private def bytes2hex(bytes: Array[Byte], sep: String): String = {
    sep + bytes.map("%02x".format(_)).mkString(sep)
  }

  private def hexStringToByteArray(s: String): Array[Byte] = {
    val sn = s.replace("\\x", "")
    val b: Array[Byte] = new Array[Byte](sn.length / 2)
    var i: Int = 0
    while (i < b.length) {
      {
        val index: Int = i * 2
        val v: Int = Integer.parseInt(sn.substring(index, index + 2), 16)
        b(i) = v.toByte
      }
      {
        i += 1
        i - 1
      }
    }
    b
  }

}
