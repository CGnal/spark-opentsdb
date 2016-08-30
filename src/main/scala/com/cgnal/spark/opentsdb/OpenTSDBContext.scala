/*
 * Copyright 2016 CGnal S.p.A.
 *
 */

package com.cgnal.spark.opentsdb

import java.io.File
import java.nio.file.{ Files, Paths }
import java.sql.Timestamp
import java.time.{ Instant, LocalDateTime, ZoneId, ZonedDateTime }
import java.util

import com.cloudera.sparkts.{ DateTimeIndex, Frequency, TimeSeriesRDD }
import net.opentsdb.core.{ IllegalDataException, Internal, TSDB }
import net.opentsdb.uid.UniqueId.UniqueIdType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.streaming.dstream.DStream
import shaded.org.hbase.async.KeyValue

import scala.collection.convert.decorateAsScala._
import scala.collection.mutable.ListBuffer
import scala.language.{ postfixOps, reflectiveCalls }

/**
 * A class representing a single datapoint
 *
 * @param metric    the metric name the data point belongs to
 * @param timestamp the data point's timestamp
 * @param value     tha value
 * @param tags      the metric tags
 * @tparam T the actual value type
 */
final case class DataPoint[T <: AnyVal](metric: String, timestamp: Long, value: T, tags: Map[String, String]) extends Serializable

/**
 * This companion object is used for carrying important TSDB configuration properties
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object OpenTSDBContext {

  /**
   * The HBase table containing the metrics
   */
  var tsdbTable = "tsdb"

  /**
   * The HBase table containing the various IDs for tags amnd metric names
   */
  var tsdbUidTable = "tsdb-uid"

  /**
   * The salting prefix width, currently it can be 0=NO SALTING or 1
   */
  var saltWidth: Int = 0

  /**
   * The number of salting buckets
   */
  var saltBuckets: Int = 0

}

/**
 * This class provides all the functionalities for reading and writing metrics from/to an OpenTSDB instance
 *
 * @param sqlContext    The sql context needed for creating the dataframes, the spark context it's obtained from this sql context
 * @param configuration The HBase configuration that can be optionally passed, if None it will be automatically retrieved from
 *                      the classpath
 */
class OpenTSDBContext(@transient sqlContext: SQLContext, @transient configuration: Option[Configuration] = None) extends Serializable {

  @transient private lazy val log = Logger.getLogger(getClass.getName)

  private val hbaseContext = new HBaseContext(sqlContext.sparkContext, configuration.getOrElse(new Configuration()))

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private[opentsdb] var tsdbTable = OpenTSDBContext.tsdbTable

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private[opentsdb] var tsdbUidTable = OpenTSDBContext.tsdbUidTable

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private[opentsdb] var saltWidth: Int = OpenTSDBContext.saltWidth

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private[opentsdb] var saltBuckets: Int = OpenTSDBContext.saltBuckets

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var keytabPath_ : Option[String] = None

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var keytabData_ : Option[Broadcast[Array[Byte]]] = None

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var principal_ : Option[String] = None

  /**
   * @return the keytab path for accessing the secure HBase
   */
  def keytab = keytabData_.getOrElse(throw new Exception("keytab has not been defined"))

  /**
   * @param keytab the path of the file containing the keytab
   */
  def keytab_=(keytab: String) = {
    val keytabPath = new File(keytab).getAbsolutePath
    val byteArray = Files.readAllBytes(Paths.get(keytabPath))
    keytabPath_ = Some(keytabPath)
    keytabData_ = Some(sqlContext.sparkContext.broadcast(byteArray))
  }

  /**
   * @return the Kerberos principal
   */
  def principal = principal_.getOrElse(throw new Exception("principal has not been defined"))

  /**
   * @param principal the kerberos principal to be used in combination with the keytab
   */
  def principal_=(principal: String) = principal_ = Some(principal)

  /**
   * It loads multiple OpenTSDB timeseries into a [[com.cloudera.sparkts.TimeSeriesRDD]]
   *
   * @param interval  an optional pair of longs, the first long is the epoch time in seconds as the beginning of the interval,
   *                  the second long is the end of the interval (exclusive).
   *                  This method will retrieve all the metrics included into this interval.
   * @param frequency the interval frequency, see `Frequency`
   * @param metrics   a list of pair metric name, tags
   * @return a [[com.cloudera.sparkts.TimeSeriesRDD]] instance
   */
  def loadTimeSeriesRDD(
    interval: Option[(Long, Long)],
    frequency: Frequency,
    metrics: List[(String, Map[String, String])]
  ): TimeSeriesRDD[String] = {

    val startDateEpochInMillis: Long = new Timestamp(interval.getOrElse(throw new Exception)._1.toLong * 1000).getTime
    val endDateEpochInInMillis: Long = new Timestamp(interval.getOrElse(throw new Exception)._2.toLong * 1000).getTime - 1

    LocalDateTime.ofInstant(Instant.ofEpochMilli(startDateEpochInMillis), ZoneId.of("Z"))

    val startZoneDate = ZonedDateTime.of(LocalDateTime.ofInstant(Instant.ofEpochMilli(startDateEpochInMillis), ZoneId.of("Z")), ZoneId.of("Z"))
    val endZoneDate = ZonedDateTime.of(LocalDateTime.ofInstant(Instant.ofEpochMilli(endDateEpochInInMillis), ZoneId.of("Z")), ZoneId.of("Z"))

    val index = DateTimeIndex.uniformFromInterval(startZoneDate, endZoneDate, frequency, ZoneId.of("UTC")).atZone(ZoneId.of("UTC"))

    val dfs: List[DataFrame] = metrics.map(m => loadDataFrame(
      m._1,
      m._2,
      interval
    ))

    val initDF = dfs.headOption.getOrElse(throw new Exception("There must be at least one dataframe"))
    val otherDFs = dfs.drop(1)

    val observations = if (otherDFs.isEmpty)
      initDF
    else
      otherDFs.fold(initDF)((df1, df2) => df1.unionAll(df2))

    TimeSeriesRDD.timeSeriesRDDFromObservations(
      index,
      observations,
      "timestamp",
      "metric",
      "value"
    )
  }

  /**
   * This method loads a time series from OpenTSDB as a [[org.apache.spark.sql.DataFrame]]
   *
   * @param metricName the metric name
   * @param tags       the metric tags
   * @param interval   an optional pair of longs, the first long is the epoch time in seconds as the beginning of the interval,
   *                   the second long is the end of the interval (exclusive).
   *                   This method will retrieve all the metrics included into this interval.
   * @return the data frame
   */
  def loadDataFrame(
    metricName: String,
    tags: Map[String, String] = Map.empty[String, String],
    interval: Option[(Long, Long)] = None
  ): DataFrame = {
    val schema = StructType(
      Array(
        StructField("timestamp", TimestampType, nullable = false),
        StructField("metric", StringType, nullable = false),
        StructField("value", DoubleType, nullable = false),
        StructField("tags", DataTypes.createMapType(StringType, StringType), nullable = false)
      )
    )

    val rowRDD = load(metricName, tags, interval, ConvertToDouble).map[Row] {
      dp =>
        Row(
          new Timestamp(dp.timestamp),
          dp.metric,
          dp.value.asInstanceOf[Double],
          dp.tags
        )
    }
    sqlContext.createDataFrame(rowRDD, schema)
  }

  /**
   * This method loads a time series from OpenTSDB as a [[RDD]][ [[DataPoint]] ]
   *
   * @param metricName         the metric name
   * @param tags               the metric tags
   * @param interval           an optional pair of longs, the first long is the epoch time in seconds as the beginning of the interval,
   *                           the second long is the end of the interval (exclusive).
   *                           This method will retrieve all the metrics included into this interval.
   * @param conversionStrategy if `NoConversion` the `DataPoint`'s value type will the actual one, as retrieved from the storage,
   *                           otherwise, if `ConvertToDouble` the value will be converted to Double
   * @return the `RDD`
   */
  def load(
    metricName: String,
    tags: Map[String, String] = Map.empty[String, String],
    interval: Option[(Long, Long)] = None,
    conversionStrategy: ConversionStrategy = NoConversion
  ): RDD[DataPoint[_ <: AnyVal]] = {

    log.info("Loading metric and tags uids")

    TSDBClientManager.init(
      keytabPath = keytabPath_,
      keytabData = keytabData_,
      principal = principal_,
      hbaseContext = hbaseContext,
      tsdbTable = tsdbTable,
      tsdbUidTable = tsdbUidTable,
      saltWidth = saltWidth,
      saltBuckets = saltBuckets
    )

    val tsdb = TSDBClientManager.tsdb.getOrElse(throw new Exception("the TSDB client instance has not been initialised correctly"))

    val metricsUID = tsdb.getUID(UniqueIdType.METRIC, metricName)

    val tagKUIDs: Map[String, Array[Byte]] = tags.keys.map(key => (key, tsdb.getUID(UniqueIdType.TAGK, key))).toMap

    val tagVUIDs: Map[String, Array[Byte]] = tags.values.map(value => (value, tsdb.getUID(UniqueIdType.TAGV, value))).toMap

    TSDBClientManager.shutdown()

    val rows = if (saltWidth == 0) {
      log.trace("computing hbase rows without salting")
      val metricScan = getMetricScan(
        -1: Byte,
        tags,
        metricsUID,
        tagKUIDs,
        tagVUIDs,
        interval
      )
      hbaseContext.hbaseRDD(TableName.valueOf(tsdbTable), metricScan).asInstanceOf[RDD[(ImmutableBytesWritable, Result)]]
    } else {
      assert(saltWidth == 1)
      assert(saltBuckets >= 1)
      log.trace("computing hbase rows with salting")
      val rdds = (0 until saltBuckets) map {
        bucket =>
          val metricScan = getMetricScan(
            bucket.toByte,
            tags,
            metricsUID,
            tagKUIDs,
            tagVUIDs,
            interval
          )
          hbaseContext.hbaseRDD(TableName.valueOf(tsdbTable), metricScan).asInstanceOf[RDD[(ImmutableBytesWritable, Result)]]
      } toList

      val initRDD = rdds.headOption.getOrElse(throw new Exception("There must be at least one RDD"))
      val otherRDDs = rdds.drop(1)

      if (otherRDDs.isEmpty)
        initRDD
      else
        otherRDDs.fold(initRDD)((rdd1, rdd2) => rdd1.union(rdd2))
    }

    val rdd = rows.mapPartitions[Iterator[DataPoint[_ <: AnyVal]]](f = iterator => {
      TSDBClientManager.init(
        keytabPath = keytabPath_,
        keytabData = keytabData_,
        principal = principal_,
        hbaseContext = hbaseContext,
        tsdbTable = tsdbTable,
        tsdbUidTable = tsdbUidTable,
        saltWidth = saltWidth,
        saltBuckets = saltBuckets
      )
      new Iterator[Iterator[DataPoint[_ <: AnyVal]]] {

        val tsdb = TSDBClientManager.tsdb.getOrElse(throw new Exception("the TSDB client instance has not been initialised correctly"))

        val i = iterator.map(row => process(row, tsdb, interval, conversionStrategy))

        override def hasNext =
          if (!i.hasNext) {
            log.trace("iterating done, calling shutdown on the TSDB client instance")
            TSDBClientManager.shutdown()
            false
          } else
            i.hasNext

        override def next() = i.next()
      }
    }, preservesPartitioning = true)

    rdd.flatMap(identity[Iterator[DataPoint[_ <: AnyVal]]])
  }

  private def process(row: (ImmutableBytesWritable, Result), tsdb: TSDB, interval: Option[(Long, Long)], conversionStrategy: ConversionStrategy): Iterator[DataPoint[_ <: AnyVal]] = {
    log.trace("processing row")
    val key = row._1.get()
    val metric = Internal.metricName(tsdb, key)
    val baseTime = Internal.baseTime(tsdb, key)
    val tags = Internal.getTags(tsdb, key).asScala
    val dps = new ListBuffer[DataPoint[_ <: AnyVal]]
    row._2.rawCells().foreach[Unit](cell => {
      val family = util.Arrays.copyOfRange(cell.getFamilyArray, cell.getFamilyOffset, cell.getFamilyOffset + cell.getFamilyLength)
      val qualifier = util.Arrays.copyOfRange(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierOffset + cell.getQualifierLength)
      val value = util.Arrays.copyOfRange(cell.getValueArray, cell.getValueOffset, cell.getValueOffset + cell.getValueLength)
      val kv = new KeyValue(key, family, qualifier, cell.getTimestamp, value)
      if (qualifier.length == 2 || qualifier.length == 4 && Internal.inMilliseconds(qualifier)) {
        val cell = Internal.parseSingleValue(kv)
        if (cell == null) {
          throw new IllegalDataException("Unable to parse row: " + kv)
        }
        val ts = cell.absoluteTimestamp(baseTime)
        val isInTheInterval = interval.fold(true)(
          interval => if (Internal.inMilliseconds(cell.qualifier()))
            ts >= interval._1 * 1000 && ts < interval._2 * 1000
          else
            ts >= interval._1 && ts < interval._2
        )
        if (isInTheInterval)
          dps += (conversionStrategy match {
            case ConvertToDouble => DataPoint(metric, ts, cell.parseValue().doubleValue(), tags.toMap)
            case NoConversion => if (cell.isInteger)
              DataPoint(metric, cell.absoluteTimestamp(baseTime), cell.parseValue().longValue(), tags.toMap)
            else
              DataPoint(metric, cell.absoluteTimestamp(baseTime), cell.parseValue().doubleValue(), tags.toMap)
          })
      } else {
        // compacted column
        log.trace("processing compacted row")
        val cells = new ListBuffer[Internal.Cell]
        try {
          cells ++= Internal.extractDataPoints(kv).asScala
        } catch {
          case e: IllegalDataException =>
            throw new IllegalDataException(Bytes.toStringBinary(key), e)
        }
        cells.foreach[Unit](cell => {
          val ts = cell.absoluteTimestamp(baseTime)
          val isInTheInterval = interval.fold(true)(
            interval => if (Internal.inMilliseconds(cell.qualifier()))
              ts >= interval._1 * 1000 && ts < interval._2 * 1000
            else
              ts >= interval._1 && ts < interval._2
          )
          if (isInTheInterval)
            dps += (conversionStrategy match {
              case ConvertToDouble => DataPoint(metric, ts, cell.parseValue().doubleValue(), tags.toMap)
              case NoConversion => if (cell.isInteger)
                DataPoint(metric, ts, cell.parseValue().longValue(), tags.toMap)
              else
                DataPoint(metric, ts, cell.parseValue().doubleValue(), tags.toMap)
            })
          ()
        })
        log.trace(s"processed ${cells.length} cells")
      }
      ()
    })
    log.trace("processing row: done")
    dps.iterator
  }

  /**
   * It writes a [[RDD]][ [[DataPoint]] ] back to OpenTSDB
   *
   * @param timeseries the [[RDD]] of [[DataPoint]]s to be stored
   * @param writeFunc  the implicit writefunc to be used for a specific value type
   * @tparam T the actual type of the `DataPoint`'s value
   */
  def write[T <: AnyVal](timeseries: RDD[DataPoint[T]])(implicit writeFunc: (Iterator[DataPoint[T]], TSDB) => Unit): Unit = {
    timeseries.foreachPartition(it => {
      TSDBClientManager.init(
        keytabPath = keytabPath_,
        keytabData = keytabData_,
        principal = principal_,
        hbaseContext = hbaseContext,
        tsdbTable = tsdbTable,
        tsdbUidTable = tsdbUidTable,
        saltWidth = saltWidth,
        saltBuckets = saltBuckets
      )
      writeFunc(
        new Iterator[DataPoint[T]] {
          override def hasNext =
            if (!it.hasNext) {
              log.trace("iterating done, calling shutdown on the TSDB client instance")
              TSDBClientManager.shutdown()
              false
            } else
              it.hasNext

          override def next() = it.next()
        }, TSDBClientManager.tsdb.getOrElse(throw new Exception("the TSDB client instance has not been initialised correctly"))
      )
    })
  }

  /**
   * It writes a [[DataFrame]] back to OpenTSDB
   *
   * @param timeseries the data frame to be stored
   * @param writeFunc  the implicit writefunc to be used for a specific value type
   */
  def write(timeseries: DataFrame)(implicit writeFunc: (Iterator[DataPoint[Double]], TSDB) => Unit): Unit = {
    assert(timeseries.schema == StructType(
      Array(
        StructField("timestamp", TimestampType, nullable = false),
        StructField("metric", StringType, nullable = false),
        StructField("value", DoubleType, nullable = false),
        StructField("tags", DataTypes.createMapType(StringType, StringType), nullable = false)
      )
    ))
    timeseries.foreachPartition(it => {
      TSDBClientManager.init(
        keytabPath = keytabPath_,
        keytabData = keytabData_,
        principal = principal_,
        hbaseContext = hbaseContext,
        tsdbTable = tsdbTable,
        tsdbUidTable = tsdbUidTable,
        saltWidth = saltWidth,
        saltBuckets = saltBuckets
      )
      writeFunc(
        new Iterator[DataPoint[Double]] {
          override def hasNext =
            if (!it.hasNext) {
              log.trace("iterating done, calling shutdown on the TSDB client instance")
              TSDBClientManager.shutdown()
              false
            } else
              it.hasNext

          override def next() = {
            val row = it.next()
            DataPoint(
              row.getAs[String]("metric"),
              row.getAs[Timestamp]("timestamp").getTime,
              row.getAs[Double]("value"),
              row.getAs[Map[String, String]]("tags")
            )
          }
        }, TSDBClientManager.tsdb.getOrElse(throw new Exception("the TSDB client instance has not been initialised correctly"))
      )
    })
  }

  /**
   * It writes a [[DStream]][ [[DataPoint]] ] back to OpenTSDB
   *
   * @param dstream   the distributed stream
   * @param writeFunc the implicit writefunc to be used for a specific value type
   * @tparam T the actual type of the [[DataPoint]]'s value
   */
  def streamWrite[T <: AnyVal](dstream: DStream[DataPoint[T]])(implicit writeFunc: (Iterator[DataPoint[T]], TSDB) => Unit): Unit = {
    dstream foreachRDD {
      timeseries =>
        timeseries foreachPartition {
          it =>
            TSDBClientManager.init(
              keytabPath = keytabPath_,
              keytabData = keytabData_,
              principal = principal_,
              hbaseContext = hbaseContext,
              tsdbTable = tsdbTable,
              tsdbUidTable = tsdbUidTable,
              saltWidth = saltWidth,
              saltBuckets = saltBuckets
            )
            writeFunc(
              new Iterator[DataPoint[T]] {
                override def hasNext =
                  if (!it.hasNext) {
                    log.trace("iterating done, calling shutdown on the TSDB client instance")
                    TSDBClientManager.shutdown()
                    false
                  } else
                    it.hasNext

                override def next() = it.next()
              }, TSDBClientManager.tsdb.getOrElse(throw new Exception("the TSDB client instance has not been initialised correctly"))
            )
        }
    }
  }

}
