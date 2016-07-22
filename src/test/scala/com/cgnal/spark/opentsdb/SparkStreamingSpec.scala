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

import org.apache.spark.sql.types._
import org.apache.spark.streaming.{ Milliseconds, StreamingContext }

import scala.collection.mutable

class SparkStreamingSpec extends SparkBaseSpec {

  "Spark" must {
    "save timeseries points from a Spark stream correctly" in {

      val points = for {
        i <- 0 until 10
        ts = Timestamp.from(Instant.parse(s"2016-07-05T${10 + i}:00:00.00Z"))
        epoch = ts.getTime
        point = ("mymetric", epoch, i.toDouble, Map("key1" -> "value1", "key2" -> "value2"))
      } yield point

      val rdd = sparkContext.parallelize[(String, Long, Double, Map[String, String])](points)

      val streamingContext = new StreamingContext(sparkContext, Milliseconds(200))

      val stream = streamingContext.queueStream[(String, Long, Double, Map[String, String])](mutable.Queue(rdd))

      openTSDBContext.streamWrite(stream)
      streamingContext.start()

      Thread.sleep(1000)

      val simpleDateFormat = new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm")
      simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
      val df = openTSDBContext.loadDataFrame("mymetric", Map("key1" -> "value1", "key2" -> "value2"), Some("05/07/2016 10:00"), Some("05/07/2016 20:00"), conversionStrategy = ConvertToFloat)

      df.schema must be(StructType(Array(StructField("timestamp", TimestampType, false), StructField("value", FloatType, false))))

      val result = df.collect()

      result.length must be(10)

      result.foreach(println(_))
      streamingContext.stop(false)
    }
  }

}