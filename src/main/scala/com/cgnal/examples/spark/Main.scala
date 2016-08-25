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

import java.io.File
import java.sql.Timestamp
import java.time.Instant

import com.cgnal.spark.opentsdb.{ OpenTSDBContext, _ }
import org.apache.spark.sql.SQLContext
import org.apache.spark.{ SparkConf, SparkContext }

/*
spark-submit --executor-memory 1200M \
  --driver-class-path /etc/hbase/conf \
  --conf spark.executor.extraClassPath=/etc/hbase/conf \
  --conf spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/etc/hbase/conf/jaas.conf \
  --master yarn --deploy-mode client \
  --class com.cgnal.examples.spark.Main spark-opentsdb-assembly-1.0.jar xxxx dgreco.keytab dgreco@VMCLUSTER
 */

object Main extends App {

  val yarn = false

  val initialExecutors = 1

  val minExecutors = 1

  val conf = new SparkConf().setAppName("spark-cdh5-template-yarn")

  val master = conf.getOption("spark.master")

  val uberJarLocation = {
    val location = getJar(Main.getClass)
    if (new File(location).isDirectory) s"${System.getProperty("user.dir")}/assembly/target/scala-2.10/spark-opentsdb-assembly-1.0.jar" else location
  }

  if (master.isEmpty) {
    //it means that we are NOT using spark-submit

    addPath(args(0))

    if (yarn)
      conf.
        setMaster("yarn-client").
        setAppName("spark-cdh5-template-yarn").
        setJars(List(uberJarLocation)).
        set("spark.yarn.jar", "local:/opt/cloudera/parcels/CDH/lib/spark/assembly/lib/spark-assembly.jar").
        set("spark.executor.extraClassPath", "/opt/cloudera/parcels/CDH/jars/*"). //This is an hack I made I'm not sure why it works though
        set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
        set("spark.io.compression.codec", "lzf").
        set("spark.speculation", "true").
        set("spark.shuffle.manager", "sort").
        set("spark.shuffle.service.enabled", "true").
        set("spark.dynamicAllocation.enabled", "true").
        set("spark.dynamicAllocation.initialExecutors", Integer.toString(initialExecutors)).
        set("spark.dynamicAllocation.minExecutors", Integer.toString(minExecutors)).
        set("spark.executor.cores", Integer.toString(1)).
        set("spark.executor.memory", "256m")
    else
      conf.
        setAppName("spark-cdh5-template-local").
        setMaster("local")
  }

  val sparkContext = new SparkContext(conf)
  implicit val sqlContext = new SQLContext(sparkContext)

  val points = for {
    i <- 0 until 10
    ts = Timestamp.from(Instant.parse(s"2016-07-05T${10 + i}:00:00.00Z"))
    epoch = ts.getTime
    point = DataPoint("mymetric1", epoch, i.toDouble, Map("key1" -> "value1", "key2" -> "value2"))
  } yield point

  val rdd = sparkContext.parallelize[DataPoint[Double]](points)

  rdd.toDF.write.options(Map(
    "opentsdb.keytab" -> args(1),
    "opentsdb.principal" -> args(2)
  )).mode("append").opentsdb

  val tsStart = Timestamp.from(Instant.parse(s"2016-07-05T10:00:00.00Z")).getTime / 1000
  val tsEnd = Timestamp.from(Instant.parse(s"2016-07-05T20:00:00.00Z")).getTime / 1000

  val df = sqlContext.read.options(Map(
    "opentsdb.metric" -> "mymetric1",
    "opentsdb.tags" -> "key->value1,key2->value2",
    "opentsdb.interval" -> s"$tsStart:$tsEnd",
    "opentsdb.keytab" -> args(1),
    "opentsdb.principal" -> args(2)
  )).opentsdb

  val result = df.collect()

  result.foreach(println(_))

  sparkContext.stop()
}
