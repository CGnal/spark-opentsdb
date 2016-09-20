/*
 * Copyright 2016 CGnal S.p.A.
 *
 */

package com.cgnal.examples.spark

import java.io.File
import java.sql.Timestamp
import java.time.Instant

import com.cgnal.spark.opentsdb._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{ SparkConf, SparkContext }

import scala.util.Random

/*
  spark-submit --executor-memory 1200M \
  --driver-class-path /etc/hbase/conf \
  --conf spark.executor.extraClassPath=/etc/hbase/conf \
  --conf spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/tmp/jaas.conf \
  --master yarn --deploy-mode client \
  --keytab dgreco.keytab \
  --principal dgreco@DGRECO-MBP.LOCAL \
  --class com.cgnal.examples.spark.Main spark-opentsdb-assembly-1.0.jar xxxx dgreco.keytab dgreco@DGRECO-MBP.LOCAL
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

  sparkContext.setLogLevel("ERROR")

  implicit val sqlContext: SQLContext = new SQLContext(sparkContext)

  val ts = Timestamp.from(Instant.parse(s"2016-07-05T10:00:00.00Z"))
  val N = 1000000
  val M = 10
  val r = new Random
  val points = for {
    i <- 0 until N
    epoch = ts.getTime + (i * 1000)
    point = DataPoint(s"mymetric${r.nextInt(M)}", epoch, i.toDouble, Map("key1" -> "value1", "key2" -> "value2"))
  } yield point

  val rdd = sparkContext.parallelize[DataPoint[Double]](points)

  OpenTSDBContext.saltWidth = 1
  OpenTSDBContext.saltBuckets = 4

  val start = System.currentTimeMillis()
  rdd.toDF.write.options(Map(
    "opentsdb.keytabLocalTempDir" -> "/tmp",
    "opentsdb.keytab" -> args(1),
    "opentsdb.principal" -> args(2)
  )).mode("append").opentsdb()
  val stop = System.currentTimeMillis()
  println(s"$N data point written in ${stop - start} milliseconds")

  val tsStart = Timestamp.from(Instant.parse(s"2016-07-05T09:00:00.00Z")).getTime / 1000
  val tsEnd = Timestamp.from(Instant.parse(s"2016-07-05T20:00:00.00Z")).getTime / 1000

  val tot = (0 until M).map(i => {
    val df = sqlContext.read.options(Map(
      "opentsdb.metric" -> s"mymetric$i",
      "opentsdb.tags" -> "key1->value1,key2->value2",
      "opentsdb.keytabLocalTempDir" -> "/tmp",
      "opentsdb.keytab" -> args(1),
      "opentsdb.principal" -> args(2)
    )).opentsdb

    val start = System.currentTimeMillis()
    val count = df.count()
    val stop = System.currentTimeMillis()
    println(s"$count data point retrieved in ${stop - start} milliseconds")
    count
  }).sum

  println(tot)

  sparkContext.stop()
}
