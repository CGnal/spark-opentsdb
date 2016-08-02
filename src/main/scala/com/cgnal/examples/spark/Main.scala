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

import com.cgnal.spark.opentsdb.OpenTSDBContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.{ SparkConf, SparkContext }

object Main extends App {

  val yarn = true

  val initialExecutors = 4

  val minExecutors = 4

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
  val hbaseContext = new HBaseContext(sparkContext, new Configuration())
  val openTSDBContext = new OpenTSDBContext(hbaseContext)

  val df = openTSDBContext.loadDataFrame(new SQLContext(sparkContext), "open", Map("symbol" -> "AAPL"), Some("06/06/2016 20:00"), Some("27/06/2016 17:00"))

  val result = df.collect()

  result.foreach(println(_))

  println(result.length)

  sparkContext.stop()

}
